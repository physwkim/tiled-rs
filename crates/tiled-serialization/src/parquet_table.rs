//! Parquet serializer for `table` family.
//!
//! Round-trips Arrow IPC bytes through `parquet::arrow::ArrowWriter` so
//! the wire format produced by the table partition handler can be
//! re-served as parquet on demand. A streaming encoder would be more
//! memory-efficient for huge partitions; sufficient for now is a
//! buffered encode (matches what tiled-server already does for arrow IPC).

#![cfg(feature = "parquet")]

use std::io::Cursor;

use arrow::ipc::reader::FileReader;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use tiled_core::media_type::mime;
use tiled_core::structures::StructureFamily;

use crate::registry::{SerializationRegistry, SerializerFn};

pub fn register_parquet_serializer(reg: &SerializationRegistry) {
    reg.register(StructureFamily::Table, mime::PARQUET, parquet_serializer());
    reg.register_alias(".parquet", mime::PARQUET);
    reg.register_alias(".pq", mime::PARQUET);
}

fn parquet_serializer() -> SerializerFn {
    Box::new(|data, _meta| -> Result<Bytes, crate::registry::SerializeError> {
        // The table handler emits Arrow IPC bytes; round-trip through the
        // IPC reader so we have a SchemaRef + RecordBatches to feed to
        // ArrowWriter.
        let cursor = Cursor::new(data.to_vec());
        let reader =
            FileReader::try_new(cursor, None).map_err(|e| format!("ipc reader: {e}"))?;
        let schema = reader.schema();
        let batches: Vec<_> = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("ipc batches: {e}"))?;

        let mut buf = Vec::new();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props))
                .map_err(|e| format!("parquet writer: {e}"))?;
            for batch in &batches {
                writer
                    .write(batch)
                    .map_err(|e| format!("parquet write: {e}"))?;
            }
            writer.close().map_err(|e| format!("parquet close: {e}"))?;
        }
        Ok(Bytes::from(buf))
    })
}
