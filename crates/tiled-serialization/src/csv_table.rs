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

pub fn register_csv_table_serializer(reg: &SerializationRegistry) {
    reg.register(StructureFamily::Table, mime::CSV, csv_table_serializer());
    reg.register_alias(".csv", mime::CSV);
}

fn csv_table_serializer() -> SerializerFn {
    Box::new(
        |data, _meta| -> Result<Bytes, crate::registry::SerializeError> {
            let cursor = Cursor::new(data.to_vec());
            let reader =
                FileReader::try_new(cursor, None).map_err(|e| format!("ipc reader: {e}"))?;
            let mut buf = Vec::new();
            {
                let mut writer = WriterBuilder::new().with_header(true).build(&mut buf);
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
