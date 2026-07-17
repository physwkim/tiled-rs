//! Excel (.xlsx) workbook read adapter.
//!
//! A workbook is exposed as a **container** of per-sheet tables — one child
//! table per worksheet, keyed by sheet name — mirroring upstream
//! `ExcelAdapter(MapAdapter[TableAdapter])` (`tiled/adapters/excel.py:14`,
//! `:52`), which loops every `sheet_name` in the workbook and maps each onto a
//! `DataFrameAdapter`. The mapping is unconditional: even a single-sheet
//! workbook is a one-child container, never a bare table. (The former port
//! read only the first worksheet and served it as a lone table, silently
//! dropping every other sheet.)
//!
//! Each per-sheet table infers its schema from the data: columns containing
//! only integers become Int64, columns containing only numbers (int or float)
//! become Float64, everything else becomes Utf8 (including mixed and error
//! cells). Empty cells become Arrow nulls. The first non-empty row is used as
//! column headers.
//!
//! Read-only. Write is handled by the `tiled-serialization` Excel serializer
//! (hand-rolled XLSX zipper).

#![cfg(feature = "excel-adapter")]

use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use calamine::{Data, Range, Reader, Xlsx, open_workbook_from_rs};
use indexmap::IndexMap;

use crate::adapters::MapAdapter;
use crate::core::adapters::{
    AnyAdapter, BaseAdapter, BoxFuture, TableAdapterRead, TableAdapterWrite,
};
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

/// Entry point: open an `.xlsx` workbook as a container of per-sheet tables.
pub struct ExcelAdapter;

impl ExcelAdapter {
    /// Open every worksheet of `path` as a [`MapAdapter`] container of per-sheet
    /// tables (child key = sheet name).
    ///
    /// Parity with upstream `ExcelAdapter(MapAdapter[TableAdapter])`
    /// (`excel.py:14`, `:52-57`): the workbook maps unconditionally onto a
    /// container with one table child per sheet — a single-sheet workbook is a
    /// one-child container, not a bare table. `metadata` is the catalog node's
    /// metadata; it lands on the container (upstream passes `metadata=node`),
    /// while each per-sheet table carries no metadata/specs of its own (upstream
    /// builds each child as a bare `DataFrameAdapter.from_dask_dataframe`).
    pub fn from_path(path: PathBuf, metadata: serde_json::Value) -> Result<MapAdapter> {
        let sheets = read_workbook(&path)?;
        let mut mapping: IndexMap<String, AnyAdapter> = IndexMap::with_capacity(sheets.len());
        for (name, schema, rows) in sheets {
            let sheet = ExcelSheetAdapter::new(schema, rows);
            mapping.insert(name, AnyAdapter::Table(Arc::new(sheet)));
        }
        // The `xlsx` spec identifies the container as an Excel-file node; the
        // per-sheet tables are plain tables.
        Ok(MapAdapter::new(mapping, metadata, vec![Spec::new("xlsx")]))
    }
}

/// A single worksheet exposed as a table (one partition). One of these is
/// synthesized per sheet by [`ExcelAdapter::from_path`]; it is the
/// `TableAdapter`-equivalent child upstream builds with `DataFrameAdapter`.
#[derive(Debug)]
pub struct ExcelSheetAdapter {
    schema: SchemaRef,
    /// Pre-loaded data rows (calamine reads the whole file eagerly anyway).
    rows: Vec<Vec<CellKind>>,
    structure: TableStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl ExcelSheetAdapter {
    /// Build a per-sheet table adapter from an already-parsed schema + rows.
    fn new(schema: SchemaRef, rows: Vec<Vec<CellKind>>) -> Self {
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let structure = TableStructure {
            arrow_schema: encode_schema(schema.as_ref()),
            npartitions: 1,
            columns,
            resizable: Default::default(),
        };
        Self {
            schema,
            rows,
            structure,
            // A per-sheet child carries no user metadata/specs, matching
            // upstream's bare `DataFrameAdapter.from_dask_dataframe(ddf)`.
            metadata: serde_json::json!({}),
            specs: vec![],
        }
    }
}

impl BaseAdapter for ExcelSheetAdapter {
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

impl TableAdapterRead for ExcelSheetAdapter {
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

/// One parsed worksheet: its name, inferred Arrow schema, and data rows.
type SheetData = (String, SchemaRef, Vec<Vec<CellKind>>);

/// Open `path` as an xlsx workbook and parse **every** worksheet, in workbook
/// order, into `(sheet_name, inferred Arrow schema, data rows)`.
fn read_workbook(path: &std::path::Path) -> Result<Vec<SheetData>> {
    let file = std::fs::File::open(path)
        .map_err(|e| TiledError::Internal(format!("open {}: {e}", path.display())))?;
    let buf = BufReader::new(file);
    let mut workbook: Xlsx<_> = open_workbook_from_rs(buf)
        .map_err(|e| TiledError::Internal(format!("xlsx open {}: {e}", path.display())))?;

    // `sheet_names()` yields names in workbook-definition order; a zero-sheet
    // workbook (structurally invalid, but handled) yields an empty container,
    // matching upstream's `for sheet_name in excel_file.sheet_names` over an
    // empty list.
    let names = workbook.sheet_names().to_vec();
    let mut sheets = Vec::with_capacity(names.len());
    for name in names {
        let range = workbook
            .worksheet_range(&name)
            .map_err(|e| TiledError::Internal(format!("xlsx read sheet {name}: {e}")))?;
        let (schema, rows) = parse_range(&range);
        sheets.push((name, schema, rows));
    }
    Ok(sheets)
}

/// Parse one worksheet range into an inferred Arrow schema + all data rows as
/// `CellKind` values (first non-empty row = headers).
fn parse_range(range: &Range<Data>) -> (SchemaRef, Vec<Vec<CellKind>>) {
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
        None => return (Arc::new(Schema::empty()), vec![]),
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
    (schema, data_rows)
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

    use crate::core::adapters::{AnyAdapter, BaseAdapter, ContainerAdapter, TableAdapterRead};
    use crate::core::error::TiledError;
    use crate::core::structures::StructureFamily;

    use super::ExcelAdapter;

    /// One sheet for the test writer: `(name, headers, rows)`.
    type SheetSpec<'a> = (&'a str, Vec<&'a str>, Vec<Vec<&'a str>>);

    /// Write a minimal valid xlsx file with the given sheets. Uses the same
    /// hand-rolled xlsx structure as the excel serializer in tiled-serialization
    /// so we can test against real xlsx bytes without pulling a separate writer.
    fn write_xlsx_sheets(path: &std::path::Path, sheets: &[SheetSpec]) {
        let f = std::fs::File::create(path).unwrap();
        let mut zip = ZipWriter::new(f);
        let opts = SimpleFileOptions::default();

        // [Content_Types].xml — one worksheet override per sheet.
        let mut content_types = String::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml"  ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
"#,
        );
        for i in 0..sheets.len() {
            content_types.push_str(&format!(
                "  <Override PartName=\"/xl/worksheets/sheet{}.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>\n",
                i + 1
            ));
        }
        content_types.push_str("</Types>");
        zip.start_file("[Content_Types].xml", opts).unwrap();
        zip.write_all(content_types.as_bytes()).unwrap();

        zip.start_file("_rels/.rels", opts).unwrap();
        zip.write_all(br#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"#).unwrap();

        // workbook.xml — list every sheet, each pointing at its own relationship.
        let mut workbook = String::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/ml/2006/main"
          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
"#,
        );
        for (i, (name, _, _)) in sheets.iter().enumerate() {
            workbook.push_str(&format!(
                "    <sheet name=\"{name}\" sheetId=\"{}\" r:id=\"rId{}\"/>\n",
                i + 1,
                i + 1
            ));
        }
        workbook.push_str("  </sheets>\n</workbook>");
        zip.start_file("xl/workbook.xml", opts).unwrap();
        zip.write_all(workbook.as_bytes()).unwrap();

        // workbook.xml.rels — one relationship per worksheet part.
        let mut rels = String::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
"#,
        );
        for i in 0..sheets.len() {
            rels.push_str(&format!(
                "  <Relationship Id=\"rId{}\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet{}.xml\"/>\n",
                i + 1,
                i + 1
            ));
        }
        rels.push_str("</Relationships>");
        zip.start_file("xl/_rels/workbook.xml.rels", opts).unwrap();
        zip.write_all(rels.as_bytes()).unwrap();

        // One worksheet part per sheet.
        for (i, (_, headers, rows)) in sheets.iter().enumerate() {
            zip.start_file(format!("xl/worksheets/sheet{}.xml", i + 1), opts)
                .unwrap();
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
        }
        zip.finish().unwrap();
    }

    fn col_letter(n: usize) -> char {
        (b'A' + n as u8) as char
    }

    /// Single-sheet convenience over [`write_xlsx_sheets`] ("Sheet1").
    fn write_xlsx(path: &std::path::Path, headers: &[&str], rows: &[Vec<&str>]) {
        write_xlsx_sheets(path, &[("Sheet1", headers.to_vec(), rows.to_vec())]);
    }

    /// Pull one sheet out of the workbook container as a table adapter.
    async fn sheet(
        container: &super::MapAdapter,
        key: &str,
    ) -> std::sync::Arc<dyn TableAdapterRead> {
        match container.get(key).await.unwrap().unwrap() {
            AnyAdapter::Table(t) => t,
            other => panic!(
                "sheet {key} is {:?}, expected a table",
                other.structure_family()
            ),
        }
    }

    /// A multi-sheet workbook is a container with one table child per sheet, and
    /// each sheet is reachable as its own table with its own data — the core of
    /// the data-loss fix (former port dropped every sheet but the first).
    #[tokio::test]
    async fn multi_sheet_is_container_of_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.xlsx");
        write_xlsx_sheets(
            &path,
            &[
                (
                    "People",
                    vec!["name", "city"],
                    vec![vec!["Alice", "Seoul"], vec!["Bob", "Busan"]],
                ),
                (
                    "Scores",
                    vec!["subject", "value"],
                    vec![vec!["math", "90"], vec!["sci", "85"], vec!["art", "70"]],
                ),
            ],
        );

        let container = ExcelAdapter::from_path(path, serde_json::json!({})).unwrap();
        // Family is container (matches upstream MapAdapter), not table.
        assert_eq!(container.structure_family(), StructureFamily::Container);
        // Both sheets are present, in workbook order, keyed by sheet name.
        assert_eq!(container.keys().await.unwrap(), vec!["People", "Scores"]);
        assert_eq!(container.len().await.unwrap(), 2);

        // Sheet 1: its own columns + rows.
        let people = sheet(&container, "People").await;
        assert_eq!(people.structure().columns, vec!["name", "city"]);
        let t = people.read(None).await.unwrap();
        assert_eq!(t.batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

        // Sheet 2 — the one the old adapter dropped — is reachable with its own
        // distinct schema and row count.
        let scores = sheet(&container, "Scores").await;
        assert_eq!(scores.structure().columns, vec!["subject", "value"]);
        let t = scores.read(None).await.unwrap();
        assert_eq!(t.batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

        // An absent sheet name is a miss, not a panic.
        assert!(container.get("Nope").await.unwrap().is_none());
    }

    /// A single-sheet workbook is still a one-child container (upstream's
    /// unconditional mapping), not a bare table.
    #[tokio::test]
    async fn single_sheet_is_one_child_container() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("one.xlsx");
        write_xlsx(&path, &["a"], &[vec!["1"]]);

        let container = ExcelAdapter::from_path(path, serde_json::json!({})).unwrap();
        assert_eq!(container.structure_family(), StructureFamily::Container);
        assert_eq!(container.keys().await.unwrap(), vec!["Sheet1"]);
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

        let container = ExcelAdapter::from_path(path, serde_json::json!({})).unwrap();
        let adapter = sheet(&container, "Sheet1").await;
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

        let container = ExcelAdapter::from_path(path, serde_json::json!({})).unwrap();
        let adapter = sheet(&container, "Sheet1").await;
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
        let container = ExcelAdapter::from_path(path, serde_json::json!({})).unwrap();
        let adapter = sheet(&container, "Sheet1").await;
        let err = adapter.read(Some(&["z".into()])).await.unwrap_err();
        assert!(matches!(err, TiledError::Validation(_)));
    }

    /// Out-of-range partition is a Validation error.
    #[tokio::test]
    async fn out_of_range_partition() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oor.xlsx");
        write_xlsx(&path, &["a"], &[vec!["1"]]);
        let container = ExcelAdapter::from_path(path, serde_json::json!({})).unwrap();
        let adapter = sheet(&container, "Sheet1").await;
        assert!(adapter.read_partition(1, None).await.is_err());
    }

    /// Read-only: as_table_writable returns None.
    #[tokio::test]
    async fn is_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ro.xlsx");
        write_xlsx(&path, &["a"], &[vec!["1"]]);
        let container = ExcelAdapter::from_path(path, serde_json::json!({})).unwrap();
        let adapter = sheet(&container, "Sheet1").await;
        assert!(adapter.as_table_writable().is_none());
    }

    /// The container carries the `xlsx` spec; per-sheet tables carry none.
    #[tokio::test]
    async fn container_spec_is_xlsx() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.xlsx");
        write_xlsx(&path, &["a"], &[vec!["1"]]);
        let container = ExcelAdapter::from_path(path, serde_json::json!({})).unwrap();
        assert_eq!(container.specs()[0].name, "xlsx");
        let adapter = sheet(&container, "Sheet1").await;
        assert!(adapter.specs().is_empty());
    }

    /// Missing file is an Internal error. (`MapAdapter` is not `Debug`, so match
    /// the `Result` rather than `unwrap_err`-ing the `Ok` variant.)
    #[test]
    fn missing_file_is_error() {
        let result = ExcelAdapter::from_path(
            std::path::PathBuf::from("/no/such/file.xlsx"),
            serde_json::json!({}),
        );
        assert!(matches!(result, Err(TiledError::Internal(_))));
    }
}
