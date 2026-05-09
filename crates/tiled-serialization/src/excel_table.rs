//! Minimal Excel (XLSX) writer for `table` family.
//!
//! Produces an XLSX file with a single worksheet matching the table's
//! schema. Implementation uses a hand-rolled XLSX zipper so we don't pull
//! a heavy Excel-format crate. Cells are written as strings — clients that
//! need typed cells should re-parse the column dtype on their end.

#![cfg(feature = "csv")]

use std::io::Cursor;
use std::io::Write;

use bytes::Bytes;
use zip::write::SimpleFileOptions;
use zip::ZipWriter;

use tiled_core::structures::StructureFamily;

use crate::registry::{SerializationRegistry, SerializerFn};

pub const XLSX_MIME: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";

pub fn register_excel_serializer(reg: &SerializationRegistry) {
    reg.register(StructureFamily::Table, XLSX_MIME, excel_serializer());
    reg.register_alias(".xlsx", XLSX_MIME);
}

fn excel_serializer() -> SerializerFn {
    Box::new(|data, _meta| -> Result<Bytes, crate::registry::SerializeError> {
        // Reuse the CSV serializer's output as the source of cell values
        // — table row data is already stringified in that path.
        // For now: read the input as Arrow IPC bytes.
        use arrow::ipc::reader::FileReader;
        let cursor = Cursor::new(data.to_vec());
        let reader =
            FileReader::try_new(cursor, None).map_err(|e| format!("ipc reader: {e}"))?;
        let schema = reader.schema();
        let mut rows: Vec<Vec<String>> = Vec::new();
        for batch in reader {
            let batch = batch.map_err(|e| format!("ipc batch: {e}"))?;
            for r in 0..batch.num_rows() {
                let mut row = Vec::with_capacity(batch.num_columns());
                for c in 0..batch.num_columns() {
                    row.push(arrow_cell_string(batch.column(c).as_ref(), r));
                }
                rows.push(row);
            }
        }
        let header: Vec<String> =
            schema.fields().iter().map(|f| f.name().clone()).collect();

        let mut buf = Cursor::new(Vec::new());
        let mut zip = ZipWriter::new(&mut buf);
        let opts = SimpleFileOptions::default();

        zip.start_file("[Content_Types].xml", opts)
            .map_err(|e| format!("zip: {e}"))?;
        zip.write_all(CONTENT_TYPES_XML.as_bytes())
            .map_err(|e| format!("zip write: {e}"))?;

        zip.start_file("_rels/.rels", opts)
            .map_err(|e| format!("zip: {e}"))?;
        zip.write_all(ROOT_RELS_XML.as_bytes())
            .map_err(|e| format!("zip: {e}"))?;

        zip.start_file("xl/_rels/workbook.xml.rels", opts)
            .map_err(|e| format!("zip: {e}"))?;
        zip.write_all(WORKBOOK_RELS_XML.as_bytes())
            .map_err(|e| format!("zip: {e}"))?;

        zip.start_file("xl/workbook.xml", opts)
            .map_err(|e| format!("zip: {e}"))?;
        zip.write_all(WORKBOOK_XML.as_bytes())
            .map_err(|e| format!("zip: {e}"))?;

        zip.start_file("xl/worksheets/sheet1.xml", opts)
            .map_err(|e| format!("zip: {e}"))?;
        let sheet = render_sheet(&header, &rows);
        zip.write_all(sheet.as_bytes())
            .map_err(|e| format!("zip: {e}"))?;

        zip.finish().map_err(|e| format!("zip finish: {e}"))?;
        Ok(Bytes::from(buf.into_inner()))
    })
}

fn render_sheet(header: &[String], rows: &[Vec<String>]) -> String {
    let mut out = String::new();
    out.push_str(
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>"#,
    );
    out.push_str("<row r=\"1\">");
    for (i, h) in header.iter().enumerate() {
        out.push_str(&format!(
            "<c r=\"{}1\" t=\"inlineStr\"><is><t>{}</t></is></c>",
            col_letter(i),
            xml_escape(h)
        ));
    }
    out.push_str("</row>");
    for (rn, row) in rows.iter().enumerate() {
        out.push_str(&format!("<row r=\"{}\">", rn + 2));
        for (cn, cell) in row.iter().enumerate() {
            out.push_str(&format!(
                "<c r=\"{}{}\" t=\"inlineStr\"><is><t>{}</t></is></c>",
                col_letter(cn),
                rn + 2,
                xml_escape(cell)
            ));
        }
        out.push_str("</row>");
    }
    out.push_str("</sheetData></worksheet>");
    out
}

fn col_letter(i: usize) -> String {
    let mut n = i;
    let mut letters = Vec::new();
    loop {
        let r = n % 26;
        letters.push((b'A' + r as u8) as char);
        if n < 26 {
            break;
        }
        n = n / 26 - 1;
    }
    letters.iter().rev().collect()
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn arrow_cell_string(array: &dyn arrow::array::Array, row: usize) -> String {
    use arrow::array::Array;
    if array.is_null(row) {
        return String::new();
    }
    arrow::util::display::ArrayFormatter::try_new(array, &Default::default())
        .map(|f| f.value(row).to_string())
        .unwrap_or_default()
}

const CONTENT_TYPES_XML: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>"#;

const ROOT_RELS_XML: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"#;

const WORKBOOK_RELS_XML: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>"#;

const WORKBOOK_XML: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
<sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets>
</workbook>"#;
