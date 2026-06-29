//! Excel (.xlsx) table read adapter.
//!
//! Reads the first worksheet of an `.xlsx` file as a table family.
//! Schema is inferred from the data: columns containing only integers
//! become Int64, columns containing only numbers (int or float) become
//! Float64, everything else becomes Utf8 (including mixed and error cells).
//! Empty cells become Arrow nulls.  The first non-empty row is used as
//! column headers.
//!
//! Read-only.  Write is handled by the `tiled-serialization` Excel
//! serializer (hand-rolled XLSX zipper).

#![cfg(feature = "excel-adapter")]

use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use calamine::{Data, Reader, Xlsx, open_workbook_from_rs};

use crate::core::adapters::{BaseAdapter, BoxFuture, TableAdapterRead, TableAdapterWrite};
use crate::core::dtype::ArrowTable;
use crate::core::error::{Result, TiledError};
use crate::core::structures::{B64_ENCODED_PREFIX, Spec, StructureFamily, TableStructure};

/// Excel data cell converted to a schema-decision value.
#[derive(Clone, Debug, PartialEq)]
enum CellKind {
    Int(i64),
    Float(f64),
    Text(String),
    Null,
}

#[derive(Debug)]
pub struct ExcelAdapter {
    schema: SchemaRef,
    /// Pre-loaded data rows (calamine reads the whole file eagerly anyway).
    rows: Vec<Vec<CellKind>>,
    structure: TableStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl ExcelAdapter {
    /// Open the first worksheet of `path` and build the adapter.
    pub fn from_path(path: PathBuf, metadata: serde_json::Value) -> Result<Self> {
        let (schema, rows) = read_xlsx(&path)?;
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let structure = TableStructure {
            arrow_schema: encode_schema(schema.as_ref()),
            npartitions: 1,
            columns,
            resizable: Default::default(),
        };
        Ok(Self {
            schema,
            rows,
            structure,
            metadata,
            specs: vec![Spec::new("xlsx")],
        })
    }
}

impl BaseAdapter for ExcelAdapter {
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

impl TableAdapterRead for ExcelAdapter {
    fn structure(&self) -> &TableStructure {
        &self.structure
    }

    fn read<'a>(&'a self, fields: Option<&'a [String]>) -> BoxFuture<'a, Result<ArrowTable>> {
        Box::pin(async move {
            let batches = cells_to_batch(&self.schema, &self.rows)?;
            project(&self.schema, &batches, fields)
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
                    "excel adapter has 1 partition; got {partition}"
                )));
            }
            let batches = cells_to_batch(&self.schema, &self.rows)?;
            project(&self.schema, &batches, fields)
        })
    }

    fn as_table_writable(&self) -> Option<&dyn TableAdapterWrite> {
        None
    }
}

/// Open `path` as an xlsx workbook, read the first sheet, and return
/// the inferred Arrow schema + all data rows as `CellKind` values.
fn read_xlsx(path: &std::path::Path) -> Result<(SchemaRef, Vec<Vec<CellKind>>)> {
    let file = std::fs::File::open(path)
        .map_err(|e| TiledError::Internal(format!("open {}: {e}", path.display())))?;
    let buf = BufReader::new(file);
    let mut workbook: Xlsx<_> = open_workbook_from_rs(buf)
        .map_err(|e| TiledError::Internal(format!("xlsx open {}: {e}", path.display())))?;

    let sheet_name = workbook
        .sheet_names()
        .into_iter()
        .next()
        .ok_or_else(|| TiledError::Validation("xlsx file has no sheets".into()))?;
    let range = workbook
        .worksheet_range(&sheet_name)
        .map_err(|e| TiledError::Internal(format!("xlsx read sheet: {e}")))?;

    let mut iter = range.rows();
    // First row = headers.
    let headers: Vec<String> = match iter.next() {
        Some(row) => row
            .iter()
            .map(|cell| match cell {
                Data::String(s) => s.clone(),
                Data::Int(i) => i.to_string(),
                Data::Float(f) => f.to_string(),
                Data::Bool(b) => b.to_string(),
                Data::Empty => String::new(),
                other => format!("{other:?}"),
            })
            .collect(),
        None => return Ok((Arc::new(Schema::empty()), vec![])),
    };
    let ncols = headers.len();

    // Collect all data rows as CellKind.
    let mut data_rows: Vec<Vec<CellKind>> = Vec::new();
    for row in iter {
        let cells: Vec<CellKind> = row.iter().take(ncols).map(cell_to_kind).collect();
        // Pad short rows with nulls.
        let mut cells = cells;
        while cells.len() < ncols {
            cells.push(CellKind::Null);
        }
        data_rows.push(cells);
    }

    // Infer column types: scan all rows for each column.
    let mut col_types: Vec<ColType> = vec![ColType::Int; ncols];
    for row in &data_rows {
        for (c, cell) in row.iter().enumerate() {
            col_types[c] = col_types[c].upgrade(cell);
        }
    }

    // Build Arrow schema.
    let fields: Vec<Field> = headers
        .iter()
        .zip(col_types.iter())
        .map(|(name, t)| {
            let dt = match t {
                ColType::Int => DataType::Int64,
                ColType::Float => DataType::Float64,
                ColType::Text => DataType::Utf8,
            };
            Field::new(name, dt, true)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));
    Ok((schema, data_rows))
}

fn cell_to_kind(cell: &Data) -> CellKind {
    match cell {
        Data::Int(i) => CellKind::Int(*i),
        Data::Float(f) => CellKind::Float(*f),
        Data::Bool(b) => CellKind::Int(if *b { 1 } else { 0 }),
        Data::String(s) => {
            if s.is_empty() {
                CellKind::Null
            } else {
                CellKind::Text(s.clone())
            }
        }
        Data::DateTime(_) | Data::DateTimeIso(_) | Data::DurationIso(_) => {
            CellKind::Text(format!("{cell:?}"))
        }
        Data::Empty | Data::Error(_) => CellKind::Null,
    }
}

/// Column type lattice: Int ≤ Float ≤ Text.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ColType {
    Int,
    Float,
    Text,
}

impl ColType {
    fn upgrade(&self, cell: &CellKind) -> ColType {
        match cell {
            CellKind::Null => self.clone(),
            CellKind::Int(_) => self.clone(),
            CellKind::Float(_) => match self {
                ColType::Int => ColType::Float,
                other => other.clone(),
            },
            CellKind::Text(_) => ColType::Text,
        }
    }
}

/// Convert the pre-loaded rows into a single Arrow RecordBatch.
fn cells_to_batch(schema: &SchemaRef, rows: &[Vec<CellKind>]) -> Result<Vec<RecordBatch>> {
    if schema.fields().is_empty() {
        return Ok(vec![]);
    }
    let ncols = schema.fields().len();
    let nrows = rows.len();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(ncols);

    for c in 0..ncols {
        let dt = schema.field(c).data_type();
        let arr: ArrayRef = match dt {
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(nrows);
                for row in rows {
                    match row.get(c).unwrap_or(&CellKind::Null) {
                        CellKind::Int(i) => b.append_value(*i),
                        CellKind::Float(f) => b.append_value(*f as i64),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::with_capacity(nrows);
                for row in rows {
                    match row.get(c).unwrap_or(&CellKind::Null) {
                        CellKind::Int(i) => b.append_value(*i as f64),
                        CellKind::Float(f) => b.append_value(*f),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Utf8 => {
                let mut b = StringBuilder::with_capacity(nrows, nrows * 8);
                for row in rows {
                    match row.get(c).unwrap_or(&CellKind::Null) {
                        CellKind::Text(s) => b.append_value(s),
                        CellKind::Int(i) => b.append_value(i.to_string()),
                        CellKind::Float(f) => b.append_value(f.to_string()),
                        CellKind::Null => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            other => {
                return Err(TiledError::Internal(format!(
                    "unexpected inferred dtype {other:?} in excel adapter"
                )));
            }
        };
        arrays.push(arr);
    }
    if arrays.is_empty() || nrows == 0 {
        return Ok(vec![]);
    }
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| TiledError::Internal(format!("excel batch: {e}")))?;
    Ok(vec![batch])
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
    use std::io::Write;

    use zip::ZipWriter;
    use zip::write::SimpleFileOptions;

    use crate::core::adapters::{BaseAdapter, TableAdapterRead};
    use crate::core::error::TiledError;

    use super::ExcelAdapter;

    /// Write a minimal valid xlsx file with the given header row and data rows.
    ///
    /// Uses the same hand-rolled xlsx structure as the excel serializer in
    /// tiled-serialization so we can test against real xlsx bytes without
    /// pulling a separate writer.
    fn write_xlsx(path: &std::path::Path, headers: &[&str], rows: &[Vec<&str>]) {
        let f = std::fs::File::create(path).unwrap();
        let mut zip = ZipWriter::new(f);
        let opts = SimpleFileOptions::default();

        zip.start_file("[Content_Types].xml", opts).unwrap();
        zip.write_all(br#"<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml"  ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>"#).unwrap();

        zip.start_file("_rels/.rels", opts).unwrap();
        zip.write_all(br#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"#).unwrap();

        zip.start_file("xl/workbook.xml", opts).unwrap();
        zip.write_all(
            br#"<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/ml/2006/main"
          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Sheet1" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>"#,
        )
        .unwrap();

        zip.start_file("xl/_rels/workbook.xml.rels", opts).unwrap();
        zip.write_all(br#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>"#).unwrap();

        zip.start_file("xl/worksheets/sheet1.xml", opts).unwrap();
        let mut sheet = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        sheet.push_str("<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/ml/2006/main\"><sheetData>");
        // header row
        sheet.push_str("<row r=\"1\">");
        for (c, h) in headers.iter().enumerate() {
            sheet.push_str(&format!(
                "<c r=\"{}1\" t=\"inlineStr\"><is><t>{h}</t></is></c>",
                col_letter(c)
            ));
        }
        sheet.push_str("</row>");
        // data rows
        for (r, row) in rows.iter().enumerate() {
            let rn = r + 2;
            sheet.push_str(&format!("<row r=\"{rn}\">"));
            for (c, val) in row.iter().enumerate() {
                sheet.push_str(&format!(
                    "<c r=\"{}{rn}\" t=\"inlineStr\"><is><t>{val}</t></is></c>",
                    col_letter(c)
                ));
            }
            sheet.push_str("</row>");
        }
        sheet.push_str("</sheetData></worksheet>");
        zip.write_all(sheet.as_bytes()).unwrap();
        zip.finish().unwrap();
    }

    fn col_letter(n: usize) -> char {
        (b'A' + n as u8) as char
    }

    /// A valid xlsx with string columns: structure, full read, partition read.
    #[tokio::test]
    async fn string_columns_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.xlsx");
        write_xlsx(
            &path,
            &["name", "city"],
            &[
                vec!["Alice", "Seoul"],
                vec!["Bob", "Busan"],
                vec!["Carol", "Daegu"],
            ],
        );

        let adapter = ExcelAdapter::from_path(path, serde_json::json!({})).unwrap();
        let s = adapter.structure();
        assert_eq!(s.npartitions, 1);
        assert_eq!(s.columns, vec!["name", "city"]);

        let table = adapter.read(None).await.unwrap();
        assert_eq!(table.batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

        let part = adapter.read_partition(0, None).await.unwrap();
        assert_eq!(part.batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    /// Column projection returns only the requested column.
    #[tokio::test]
    async fn column_projection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proj.xlsx");
        write_xlsx(&path, &["x", "y"], &[vec!["1", "a"], vec!["2", "b"]]);

        let adapter = ExcelAdapter::from_path(path, serde_json::json!({})).unwrap();
        let table = adapter.read(Some(&["x".into()])).await.unwrap();
        assert_eq!(table.schema.fields().len(), 1);
        assert_eq!(table.schema.field(0).name(), "x");
    }

    /// An unknown column name is a Validation error.
    #[tokio::test]
    async fn unknown_column_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unk.xlsx");
        write_xlsx(&path, &["a"], &[vec!["1"]]);
        let adapter = ExcelAdapter::from_path(path, serde_json::json!({})).unwrap();
        let err = adapter.read(Some(&["z".into()])).await.unwrap_err();
        assert!(matches!(err, TiledError::Validation(_)));
    }

    /// Out-of-range partition is a Validation error.
    #[tokio::test]
    async fn out_of_range_partition() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oor.xlsx");
        write_xlsx(&path, &["a"], &[vec!["1"]]);
        let adapter = ExcelAdapter::from_path(path, serde_json::json!({})).unwrap();
        assert!(adapter.read_partition(1, None).await.is_err());
    }

    /// Read-only: as_table_writable returns None.
    #[test]
    fn is_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ro.xlsx");
        write_xlsx(&path, &["a"], &[vec!["1"]]);
        let adapter = ExcelAdapter::from_path(path, serde_json::json!({})).unwrap();
        assert!(adapter.as_table_writable().is_none());
    }

    /// spec name is "xlsx".
    #[test]
    fn spec_name_is_xlsx() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.xlsx");
        write_xlsx(&path, &["a"], &[vec!["1"]]);
        let adapter = ExcelAdapter::from_path(path, serde_json::json!({})).unwrap();
        assert_eq!(adapter.specs()[0].name, "xlsx");
    }

    /// Missing file is an Internal error.
    #[test]
    fn missing_file_is_error() {
        let err = ExcelAdapter::from_path(
            std::path::PathBuf::from("/no/such/file.xlsx"),
            serde_json::json!({}),
        )
        .unwrap_err();
        assert!(matches!(err, TiledError::Internal(_)));
    }
}
