//! `EventStreamTable` — exposes a Bluesky event stream as a single
//! [`TableAdapterRead`] (one Arrow `RecordBatch` per stream).
//!
//! This is the table-shaped view counterpart to [`crate::stream::EventStreamAdapter`]
//! (which is container-shaped, one column per node). Tables are easier
//! for downstream tooling that wants pandas/polars/datafusion ingestion
//! without having to fetch each column independently.
//!
//! Inline scalar columns are fully supported. External columns (with
//! `datum_id`) are NOT — they go through the array adapter path because
//! the column isn't representable as a flat Arrow value.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use mongodb::bson::{Bson, Document, doc};
use mongodb::sync::Database;

use tiled_core::adapters::{BaseAdapter, BoxFuture, TableAdapterRead};
use tiled_core::dtype::ArrowTable;
use tiled_core::error::{Result, TiledError};
use tiled_core::structures::{B64_ENCODED_PREFIX, Spec, StructureFamily, TableStructure};

pub struct EventStreamTable {
    db: Database,
    descriptors: Vec<Document>,
    cutoff_seq_num: usize,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    schema: SchemaRef,
    structure: TableStructure,
}

impl EventStreamTable {
    pub fn new(
        db: Database,
        stream_name: String,
        descriptors: Vec<Document>,
        cutoff_seq_num: usize,
    ) -> Self {
        // Aggregate column metadata across descriptors. Last writer wins
        // (matches the array adapter's first-writer-wins because Arrow
        // expects a single schema). We pick last so a stream that switched
        // dtypes mid-run picks the most recent one.
        let mut columns: Vec<(String, DataType)> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        // Always include `time` as the first column (each event has it).
        columns.push(("time".into(), DataType::Float64));
        seen.insert("time".into());
        for d in &descriptors {
            if let Ok(data_keys) = d.get_document("data_keys") {
                for (key, value) in data_keys {
                    if seen.contains(key) {
                        continue;
                    }
                    let dtype = bluesky_dtype_to_arrow(
                        value
                            .as_document()
                            .and_then(|d| d.get_str("dtype").ok())
                            .unwrap_or("number"),
                    );
                    columns.push((key.clone(), dtype));
                    seen.insert(key.clone());
                }
            }
        }
        let fields: Vec<Field> = columns
            .iter()
            .map(|(n, t)| Field::new(n, t.clone(), true))
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        let structure = TableStructure {
            arrow_schema: encode_schema(schema.as_ref()),
            npartitions: 1,
            columns: columns.iter().map(|(n, _)| n.clone()).collect(),
            resizable: Default::default(),
        };

        let descriptor_meta: Vec<serde_json::Value> = descriptors
            .iter()
            .filter_map(|d| mongodb::bson::from_document(d.clone()).ok())
            .collect();
        let metadata = serde_json::json!({
            "stream_name": stream_name,
            "descriptors": descriptor_meta,
        });

        Self {
            db,
            descriptors,
            cutoff_seq_num,
            metadata,
            specs: vec![Spec::new("xarray_dataset_table")],
            schema,
            structure,
        }
    }

    /// Returns the projected schema together with the batches so callers do
    /// not need to recompute it.  Field-name validation happens here — an
    /// unknown name in `fields` errors even for empty results.
    fn read_batches(&self, fields: Option<&[String]>) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        // Compute the target schema first so that unknown-field errors are
        // surfaced even when the result is empty.
        let target_schema: SchemaRef = match fields {
            None => self.schema.clone(),
            Some(names) => {
                let indices: Vec<usize> = names
                    .iter()
                    .map(|name| {
                        self.schema
                            .fields()
                            .iter()
                            .position(|f| f.name() == name)
                            .ok_or_else(|| {
                                TiledError::Validation(format!("unknown column: {name}"))
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Arc::new(
                    self.schema
                        .project(&indices)
                        .map_err(|e| TiledError::Internal(format!("project schema: {e}")))?,
                )
            }
        };

        if self.descriptors.is_empty() || self.cutoff_seq_num <= 1 {
            return Ok((target_schema, vec![]));
        }
        let descriptor_uids: Vec<String> = self
            .descriptors
            .iter()
            .filter_map(|d| d.get_str("uid").ok().map(String::from))
            .collect();

        let collection = self.db.collection::<Document>("event");
        let cursor = collection
            .find(doc! {
                "descriptor": { "$in": &descriptor_uids },
                "seq_num": { "$lt": self.cutoff_seq_num as i64 },
            })
            .sort(doc! { "seq_num": 1 })
            .run()
            .map_err(|e| TiledError::Internal(format!("event find: {e}")))?;

        // Per-column accumulators, keyed by name.
        let mut columns: std::collections::HashMap<String, ColumnBuilder> =
            std::collections::HashMap::new();
        for f in target_schema.fields() {
            columns.insert(f.name().clone(), ColumnBuilder::new(f.data_type()));
        }

        let mut row_count = 0usize;
        for result in cursor {
            let event = result.map_err(|e| TiledError::Internal(e.to_string()))?;
            let time = event.get_f64("time").unwrap_or(f64::NAN);
            if let Some(col) = columns.get_mut("time") {
                col.push_f64(time);
            }
            // Pull the data subdocument once per event.
            let data = event.get_document("data").ok().cloned().unwrap_or_default();
            for (name, builder) in columns.iter_mut() {
                if name == "time" {
                    continue;
                }
                builder.push_bson(data.get(name));
            }
            row_count += 1;
        }
        if row_count == 0 {
            return Ok((target_schema, vec![]));
        }
        let mut arrow_columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());
        for f in target_schema.fields() {
            let name: &str = f.name();
            let builder = columns
                .remove(name)
                .ok_or_else(|| TiledError::Internal(format!("missing builder: {name}")))?;
            arrow_columns.push(builder.finish());
        }
        let batch = RecordBatch::try_new(target_schema.clone(), arrow_columns)
            .map_err(|e| TiledError::Internal(format!("arrow batch: {e}")))?;
        Ok((target_schema, vec![batch]))
    }
}

impl BaseAdapter for EventStreamTable {
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

impl TableAdapterRead for EventStreamTable {
    fn structure(&self) -> &TableStructure {
        &self.structure
    }
    fn read<'a>(&'a self, fields: Option<&'a [String]>) -> BoxFuture<'a, Result<ArrowTable>> {
        Box::pin(async move {
            let me = clone_self(self);
            let fields = fields.map(|s| s.to_vec());
            tokio::task::spawn_blocking(move || -> Result<ArrowTable> {
                let (schema, batches) = me.read_batches(fields.as_deref())?;
                Ok(ArrowTable { schema, batches })
            })
            .await
            .map_err(|e| TiledError::Internal(format!("blocking: {e}")))?
        })
    }
    fn read_partition<'a>(
        &'a self,
        partition: usize,
        fields: Option<&'a [String]>,
    ) -> BoxFuture<'a, Result<ArrowTable>> {
        Box::pin(async move {
            if partition != 0 {
                return Err(TiledError::Validation(format!(
                    "EventStreamTable has 1 partition; got {partition}"
                )));
            }
            self.read(fields).await
        })
    }
}

fn clone_self(t: &EventStreamTable) -> EventStreamTable {
    EventStreamTable {
        db: t.db.clone(),
        descriptors: t.descriptors.clone(),
        cutoff_seq_num: t.cutoff_seq_num,
        metadata: t.metadata.clone(),
        specs: t.specs.clone(),
        schema: t.schema.clone(),
        structure: t.structure.clone(),
    }
}

fn bluesky_dtype_to_arrow(dtype: &str) -> DataType {
    match dtype {
        "integer" => DataType::Int64,
        "string" => DataType::Utf8,
        // "number", "array", "boolean" all coerced to Float64 to keep the
        // schema homogeneous (Bluesky boolean fields are commonly used as
        // 0/1 floats in downstream xarray code).
        _ => DataType::Float64,
    }
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

enum ColumnBuilder {
    F64(Vec<Option<f64>>),
    I64(Vec<Option<i64>>),
    Utf8(Vec<Option<String>>),
}

impl ColumnBuilder {
    fn new(dt: &DataType) -> Self {
        match dt {
            DataType::Int64 => Self::I64(Vec::new()),
            DataType::Utf8 => Self::Utf8(Vec::new()),
            _ => Self::F64(Vec::new()),
        }
    }

    fn push_f64(&mut self, v: f64) {
        match self {
            Self::F64(buf) => buf.push(Some(v)),
            Self::I64(buf) => buf.push(Some(v as i64)),
            Self::Utf8(buf) => buf.push(Some(v.to_string())),
        }
    }

    fn push_bson(&mut self, value: Option<&Bson>) {
        match (self, value) {
            (Self::F64(buf), Some(Bson::Double(v))) => buf.push(Some(*v)),
            (Self::F64(buf), Some(Bson::Int32(v))) => buf.push(Some(*v as f64)),
            (Self::F64(buf), Some(Bson::Int64(v))) => buf.push(Some(*v as f64)),
            (Self::F64(buf), Some(Bson::Boolean(v))) => buf.push(Some(if *v { 1.0 } else { 0.0 })),
            (Self::F64(buf), _) => buf.push(None),
            (Self::I64(buf), Some(Bson::Int64(v))) => buf.push(Some(*v)),
            (Self::I64(buf), Some(Bson::Int32(v))) => buf.push(Some(*v as i64)),
            (Self::I64(buf), Some(Bson::Double(v))) => buf.push(Some(*v as i64)),
            (Self::I64(buf), _) => buf.push(None),
            (Self::Utf8(buf), Some(Bson::String(s))) => buf.push(Some(s.clone())),
            (Self::Utf8(buf), Some(other)) => buf.push(Some(other.to_string())),
            (Self::Utf8(buf), None) => buf.push(None),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::F64(buf) => Arc::new(Float64Array::from(buf)),
            Self::I64(buf) => Arc::new(Int64Array::from(buf)),
            Self::Utf8(buf) => Arc::new(StringArray::from(buf)),
        }
    }
}
