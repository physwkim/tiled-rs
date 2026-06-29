//! Sparse serializers.
//!
//! Corresponds to `tiled/serialization/sparse.py`.
//!
//! The tiled-server converts SparseData into a COO Arrow IPC table
//! (columns dim0..dimN, data) before calling the serializer, so every
//! serializer here is identical to the table variant — just registered
//! under `StructureFamily::Sparse`.

use crate::core::media_type::mime;
use crate::core::structures::StructureFamily;

use crate::serialization::registry::SerializationRegistry;

/// Register built-in sparse serializers.
///
/// Python sparse.py delegates all DataFrame formats to the table serializers
/// after converting the sparse array to a COO DataFrame.  Rust does the same
/// conversion upstream (in tiled-server), so the serializer functions are
/// identical to those registered for `StructureFamily::Table`.
pub fn register_sparse_serializers(reg: &SerializationRegistry) {
    // Arrow IPC — canonical sparse format, mirrors APACHE_ARROW_FILE_MIME_TYPE.
    reg.register(
        StructureFamily::Sparse,
        mime::ARROW_FILE,
        Box::new(|data: &[u8], _metadata: &serde_json::Value| {
            Ok(bytes::Bytes::copy_from_slice(data))
        }),
    );

    // application/json — column-dict `{dim0:[...],...,data:[...]}`, mirroring
    // Python `serialize_json` (sparse.py:103-113): `{col: df[col].tolist()}`
    // over the COO DataFrame. Reuses the table column-dict serializer since the
    // server already hands us the COO Arrow IPC table (columns dim0..dimN,
    // data). Unconditional: serde_json is always available (Python gates this on
    // orjson; Rust has no such optional dependency).
    reg.register(
        StructureFamily::Sparse,
        mime::JSON,
        crate::serialization::table::json_table_serializer(),
    );

    // text/html — render the COO frame as an HTML <table>, mirroring Python
    // `serialize_html(to_dataframe(sparse_arr), ...)` (sparse.py:93-98), which
    // delegates to the table HTML serializer. Reuses the table HTML serializer
    // since the server already hands us the COO Arrow IPC table.
    reg.register(
        StructureFamily::Sparse,
        "text/html",
        crate::serialization::table::html_table_serializer(),
    );

    #[cfg(feature = "csv")]
    {
        for media_type in [
            mime::CSV,
            "text/x-comma-separated-values",
            mime::PLAIN,
            mime::EXCEL,
        ] {
            reg.register(
                StructureFamily::Sparse,
                media_type,
                crate::serialization::csv_table::csv_table_serializer(true),
            );
            let absent = format!("{media_type};header=absent");
            reg.register(
                StructureFamily::Sparse,
                &absent,
                crate::serialization::csv_table::csv_table_serializer(false),
            );
        }

        // Real XLSX spreadsheet (NOT the legacy `application/vnd.ms-excel` xls,
        // which maps to CSV above to match the array/table families). Mirrors
        // Python sparse.py:45-51, which registers XLSX_MIME_TYPE to
        // `serialize_excel(to_dataframe(sparse_arr), ...)`. The server already
        // hands us the COO Arrow IPC table (columns dim0..dimN, data), so the
        // table spreadsheet serializer applies unchanged.
        reg.register(
            StructureFamily::Sparse,
            crate::serialization::excel_table::XLSX_MIME,
            crate::serialization::excel_table::excel_serializer(),
        );
    }

    #[cfg(feature = "parquet")]
    reg.register(
        StructureFamily::Sparse,
        mime::PARQUET,
        crate::serialization::parquet_table::parquet_serializer(),
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::core::media_type::mime;
    use crate::core::structures::StructureFamily;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;
    use serde_json::Value;

    use crate::serialization::registry::{SerializationRegistry, default_media_type};

    fn sparse_registry() -> SerializationRegistry {
        let reg = SerializationRegistry::new();
        super::register_sparse_serializers(&reg);
        reg
    }

    /// Encode a single-batch RecordBatch into Arrow IPC bytes — the COO table
    /// (columns dim0..dimN, data) the server hands the sparse serializer.
    fn ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut w = FileWriter::try_new(&mut buf, &batch.schema()).unwrap();
            w.write(batch).unwrap();
            w.finish().unwrap();
        }
        buf
    }

    /// M3: `application/json` is registered for the sparse family
    /// (Python sparse.py:109-113).
    #[test]
    fn sparse_json_registered() {
        let reg = sparse_registry();
        assert!(
            reg.dispatch(StructureFamily::Sparse, mime::JSON).is_some(),
            "Sparse must have an application/json serializer"
        );
    }

    /// M3: the sparse JSON body is the COO column-dict
    /// `{dim0:[...],dim1:[...],data:[...]}` in DataFrame column order — dims
    /// before data, NOT alphabetized — matching Python `{col: df[col].tolist()}`
    /// over `to_dataframe` (sparse.py:55-58,104-107).
    #[test]
    fn sparse_json_is_coo_column_dict_in_order() {
        // A 2-nonzero COO frame: (dim0,dim1) coords + data values.
        let schema = Arc::new(Schema::new(vec![
            Field::new("dim0", DataType::Int64, false),
            Field::new("dim1", DataType::Int64, false),
            Field::new("data", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Float64Array::from(vec![10.0, 20.0])),
            ],
        )
        .unwrap();

        let serializer = sparse_registry()
            .dispatch(StructureFamily::Sparse, mime::JSON)
            .expect("sparse application/json must be registered");
        let out = serializer(&ipc_bytes(&batch), &serde_json::Value::Null).unwrap();
        let text = String::from_utf8(out.to_vec()).unwrap();

        // Column order dim0, dim1, data preserved (not sorted: "data" would sort
        // first). Assert on raw bytes — a re-parse re-sorts keys (no
        // serde_json `preserve_order`).
        let pos_dim0 = text.find(r#""dim0""#).expect("dim0 present");
        let pos_dim1 = text.find(r#""dim1""#).expect("dim1 present");
        let pos_data = text.find(r#""data""#).expect("data present");
        assert!(
            pos_dim0 < pos_dim1 && pos_dim1 < pos_data,
            "COO columns must keep dim0,dim1,data order: {text}"
        );

        // Values are order-independent, so re-parse is safe for them.
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["dim0"], serde_json::json!([1, 2]));
        assert_eq!(parsed["dim1"], serde_json::json!([1, 2]));
        assert_eq!(parsed["data"], serde_json::json!([10.0, 20.0]));
    }

    /// M3: `text/html` is registered for the sparse family (Python
    /// sparse.py:93-98), rendering the COO frame as an HTML <table>.
    #[test]
    fn sparse_html_registered_and_renders_coo_table() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dim0", DataType::Int64, false),
            Field::new("data", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Float64Array::from(vec![10.0, 20.0])),
            ],
        )
        .unwrap();
        let serializer = sparse_registry()
            .dispatch(StructureFamily::Sparse, "text/html")
            .expect("sparse text/html must be registered (Python sparse.py:93-98)");
        let out = serializer(&ipc_bytes(&batch), &serde_json::Value::Null).unwrap();
        let html = String::from_utf8(out.to_vec()).unwrap();
        assert!(
            html.starts_with(r#"<html><body><table border="1" class="dataframe">"#),
            "sparse html must render an HTML table: {html}"
        );
        assert!(
            html.contains("<th>dim0</th>"),
            "dim0 header present: {html}"
        );
        assert!(
            html.contains("<th>data</th>"),
            "data header present: {html}"
        );
        assert!(html.contains("<td>10.0</td>"), "data cell rendered: {html}");
    }

    #[test]
    fn sparse_default_media_type_is_arrow_file() {
        assert_eq!(
            default_media_type(StructureFamily::Sparse).as_deref(),
            Some(mime::ARROW_FILE),
        );
    }

    #[test]
    fn sparse_arrow_file_registered() {
        let reg = sparse_registry();
        assert!(
            reg.dispatch(StructureFamily::Sparse, mime::ARROW_FILE)
                .is_some(),
            "Sparse must have an Arrow IPC serializer"
        );
    }

    #[cfg(feature = "csv")]
    #[test]
    fn sparse_csv_registered() {
        let reg = sparse_registry();
        assert!(
            reg.dispatch(StructureFamily::Sparse, mime::CSV).is_some(),
            "Sparse must have a text/csv serializer"
        );
    }

    /// (Sparse, XLSX) must resolve to the real spreadsheet serializer — NOT the
    /// CSV serializer the legacy `application/vnd.ms-excel` xls maps to —
    /// mirroring Python sparse.py:45-51 (XLSX_MIME_TYPE → serialize_excel over
    /// the COO DataFrame). Output is an XLSX (a zip, magic `PK`), not CSV text.
    #[cfg(feature = "csv")]
    #[test]
    fn sparse_xlsx_registered_and_emits_spreadsheet() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dim0", DataType::Int64, false),
            Field::new("data", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Float64Array::from(vec![10.0, 20.0])),
            ],
        )
        .unwrap();
        let serializer = sparse_registry()
            .dispatch(
                StructureFamily::Sparse,
                crate::serialization::excel_table::XLSX_MIME,
            )
            .expect("sparse XLSX must be registered (Python sparse.py:45-51)");
        let out = serializer(&ipc_bytes(&batch), &Value::Null).unwrap();
        assert_eq!(
            &out[..2],
            b"PK",
            "XLSX output must be a zip (PK magic), not CSV text"
        );
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn sparse_parquet_registered() {
        let reg = sparse_registry();
        assert!(
            reg.dispatch(StructureFamily::Sparse, mime::PARQUET)
                .is_some(),
            "Sparse must have a parquet serializer"
        );
    }
}
