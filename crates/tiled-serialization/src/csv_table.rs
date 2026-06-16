//! CSV serializer for `table` family.
//!
//! Round-trips Arrow IPC bytes through `arrow::csv::WriterBuilder` so the
//! wire format produced by the table partition handler can be re-served as
//! RFC 4180 CSV on demand. Writes the header row then streams batches to
//! bound memory — no full-table `Vec<RecordBatch>` intermediate.

#![cfg(feature = "csv")]

use std::io::Cursor;

use arrow::csv::writer::WriterBuilder;
use arrow::ipc::reader::FileReader;
use bytes::Bytes;

use tiled_core::media_type::mime;
use tiled_core::structures::StructureFamily;

use crate::registry::{SerializationRegistry, SerializerFn};

/// Register table CSV serializers.
///
/// Python registers `serialize_csv` under four media types
/// (table.py:72-83) and honors the `header` MIME parameter:
/// `text/csv;header=absent` suppresses the header row
/// (table.py:59-60: `opt_params.get("header", "present") != "absent"`).
///
/// Registration strategy:
/// - Four base types (with header): text/csv, text/x-comma-separated-values,
///   text/plain, application/vnd.ms-excel.
/// - The `;header=absent` variant for each base type (without header).
///   Exact-match dispatch via `resolve_media_type` selects the right one
///   when the client sends `Accept: text/csv;header=absent`.
pub fn register_csv_table_serializer(reg: &SerializationRegistry) {
    for media_type in [
        mime::CSV,
        "text/x-comma-separated-values",
        mime::PLAIN,
        mime::EXCEL,
    ] {
        reg.register(
            StructureFamily::Table,
            media_type,
            csv_table_serializer(true),
        );
        let absent = format!("{media_type};header=absent");
        reg.register(StructureFamily::Table, &absent, csv_table_serializer(false));
    }
    reg.register_alias(".csv", mime::CSV);
}

pub(crate) fn csv_table_serializer(with_header: bool) -> SerializerFn {
    Box::new(
        move |data, _meta| -> Result<Bytes, crate::registry::SerializeError> {
            let cursor = Cursor::new(data.to_vec());
            let reader =
                FileReader::try_new(cursor, None).map_err(|e| format!("ipc reader: {e}"))?;
            let mut buf = Vec::new();
            {
                let mut writer = WriterBuilder::new()
                    .with_header(with_header)
                    .build(&mut buf);
                for batch in reader {
                    let batch = batch.map_err(|e| format!("ipc batch: {e}"))?;
                    writer
                        .write(&batch)
                        .map_err(|e| format!("csv write: {e}"))?;
                }
            }
            Ok(Bytes::from(buf))
        },
    )
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use super::*;
    use crate::registry::SerializationRegistry;

    fn make_ipc(with_header_col: bool) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut w = FileWriter::try_new(&mut buf, &schema).unwrap();
            if with_header_col {
                w.write(&batch).unwrap();
            }
            w.finish().unwrap();
        }
        buf
    }

    fn table_registry() -> SerializationRegistry {
        let reg = SerializationRegistry::new();
        register_csv_table_serializer(&reg);
        reg
    }

    /// M5: all four base media types must be registered (Python table.py:72-83).
    #[test]
    fn csv_registered_for_all_four_media_types() {
        let reg = table_registry();
        for mt in [
            mime::CSV,
            "text/x-comma-separated-values",
            mime::PLAIN,
            mime::EXCEL,
        ] {
            assert!(
                reg.dispatch(StructureFamily::Table, mt).is_some(),
                "table CSV must be registered for {mt}"
            );
        }
    }

    /// M5: header=absent variant must be registered for text/csv (Python table.py:59-60).
    #[test]
    fn csv_header_absent_variant_registered() {
        let reg = table_registry();
        assert!(
            reg.dispatch(StructureFamily::Table, "text/csv;header=absent")
                .is_some(),
            "text/csv;header=absent must be a registered serializer"
        );
    }

    /// M5: default (text/csv) includes the header row.
    #[test]
    fn csv_default_includes_header() {
        let reg = table_registry();
        let ipc = make_ipc(true);
        let ser = reg
            .dispatch(StructureFamily::Table, mime::CSV)
            .expect("text/csv registered");
        let out = ser(&ipc, &serde_json::Value::Null).unwrap();
        let text = String::from_utf8(out.to_vec()).unwrap();
        assert!(
            text.starts_with("x\n") || text.starts_with("x\r\n"),
            "default CSV must include header: got {text:?}"
        );
    }

    /// M5: text/csv;header=absent suppresses the header row.
    #[test]
    fn csv_header_absent_suppresses_header() {
        let reg = table_registry();
        let ipc = make_ipc(true);
        let ser = reg
            .dispatch(StructureFamily::Table, "text/csv;header=absent")
            .expect("text/csv;header=absent registered");
        let out = ser(&ipc, &serde_json::Value::Null).unwrap();
        let text = String::from_utf8(out.to_vec()).unwrap();
        assert!(
            !text.starts_with("x"),
            "header=absent CSV must NOT start with column name: got {text:?}"
        );
    }
}
