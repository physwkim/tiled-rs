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

use crate::core::media_type::mime;
use crate::core::structures::StructureFamily;

use crate::serialization::registry::{SerializationRegistry, SerializerFn};

pub fn register_parquet_serializer(reg: &SerializationRegistry) {
    reg.register(StructureFamily::Table, mime::PARQUET, parquet_serializer());
    reg.register_alias(".parquet", mime::PARQUET);
    reg.register_alias(".pq", mime::PARQUET);
}

pub(crate) fn parquet_serializer() -> SerializerFn {
    Box::new(
        |data, _meta| -> Result<Bytes, crate::serialization::registry::SerializeError> {
            // Stream-style: read one batch from the IPC reader, write to
            // parquet, drop. Avoids the intermediate `batches: Vec<_>` that
            // forced us to keep all batches resident at once — for large
            // partitions this halves peak memory.
            let cursor = Cursor::new(data.to_vec());
            let reader =
                FileReader::try_new(cursor, None).map_err(|e| format!("ipc reader: {e}"))?;
            let schema = reader.schema();
            let mut buf = Vec::new();
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            {
                let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))
                    .map_err(|e| format!("parquet writer: {e}"))?;
                for batch in reader {
                    let batch = batch.map_err(|e| format!("ipc batch: {e}"))?;
                    writer
                        .write(&batch)
                        .map_err(|e| format!("parquet write: {e}"))?;
                }
                writer.close().map_err(|e| format!("parquet close: {e}"))?;
            }
            Ok(Bytes::from(buf))
        },
    )
}
