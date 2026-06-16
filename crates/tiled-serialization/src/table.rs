//! Table serializers.
//!
//! Corresponds to `tiled/serialization/table.py`.

use std::io::Cursor;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::ipc::reader::FileReader;
use bytes::Bytes;
use serde_json::Value;

use tiled_core::media_type::mime;
use tiled_core::structures::StructureFamily;

use crate::registry::{SerializationRegistry, SerializeError, SerializerFn};

/// Register built-in table serializers.
pub fn register_table_serializers(registry: &SerializationRegistry) {
    // Arrow IPC format
    registry.register(
        StructureFamily::Table,
        mime::ARROW_FILE,
        Box::new(|data: &[u8], _metadata: &serde_json::Value| {
            // Data is already Arrow IPC bytes when coming from ArrowTable serialization
            Ok(bytes::Bytes::copy_from_slice(data))
        }),
    );

    // application/json: column-oriented dict `{column: [values...]}`.
    // Python registers this under `if modules_available("orjson")`
    // (table.py:141-147), which is effectively always present; orjson is not
    // optional in practice, so register unconditionally.
    registry.register(StructureFamily::Table, mime::JSON, json_table_serializer());
}

/// Column-dict JSON serializer for the table family (Python `table.py:141-147`).
///
/// Decodes the Arrow IPC bytes produced by the table partition handler into a
/// column-oriented JSON object: `{"col_a": [v0, v1, ...], "col_b": [...], ...}`.
/// Columns appear in schema (DataFrame) order, matching Python's
/// `{column: ... for column in df}`. The object is assembled by hand rather
/// than through `serde_json::Map`, because this workspace builds serde_json
/// WITHOUT `preserve_order`, so a `Map` would alphabetize the keys and lose the
/// column order Python preserves.
pub(crate) fn json_table_serializer() -> SerializerFn {
    Box::new(move |data, _meta| -> Result<Bytes, SerializeError> {
        let columns = table_ipc_to_safe_columns(data)?;
        let mut out = Vec::new();
        out.push(b'{');
        for (idx, (name, vals)) in columns.iter().enumerate() {
            if idx > 0 {
                out.push(b',');
            }
            // serde_json escapes the key as a JSON string and the values as a
            // JSON array; only the key/value ORDER is assembled by hand.
            serde_json::to_writer(&mut out, name).map_err(|e| format!("json encode: {e}"))?;
            out.push(b':');
            serde_json::to_writer(&mut out, vals.as_slice())
                .map_err(|e| format!("json encode: {e}"))?;
        }
        out.push(b'}');
        Ok(Bytes::from(out))
    })
}

/// Decode Arrow IPC bytes into JSON-safe columns, preserving schema order.
///
/// Returns one `(column_name, values)` pair per column, with `values` the
/// concatenation across all record batches. The element conversion mirrors
/// Python's `_series_to_json_safe` (table.py:113-139): missing values and
/// floating NaN/inf become JSON `null`; integers/floats/bools/strings map to
/// their JSON-native forms.
///
/// Shared by the `application/json` (column-dict) and `application/json-seq`
/// (row NDJSON) serializers, exactly as Python builds the `safe` dict once and
/// reuses it for both representations.
pub(crate) fn table_ipc_to_safe_columns(
    data: &[u8],
) -> Result<Vec<(String, Vec<Value>)>, SerializeError> {
    let cursor = Cursor::new(data.to_vec());
    let reader = FileReader::try_new(cursor, None).map_err(|e| format!("ipc reader: {e}"))?;
    let schema = reader.schema();
    let mut columns: Vec<(String, Vec<Value>)> = schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), Vec::new()))
        .collect();
    for batch in reader {
        let batch = batch.map_err(|e| format!("ipc batch: {e}"))?;
        for (col_idx, column) in columns.iter_mut().enumerate() {
            let vals = arrow_column_to_json_values(batch.column(col_idx).as_ref())?;
            column.1.extend(vals);
        }
    }
    Ok(columns)
}

/// Convert one Arrow array into a list of JSON-safe values (one per row).
///
/// Handles the column types the table adapters emit (numeric, boolean, UTF-8
/// strings) plus the Null type. `serde_json`'s `From<f32>`/`From<f64>` map NaN
/// and infinities to `Value::Null`, matching Python's NaN→`None` rule; an
/// explicit Arrow null at any index is also `null`. An unsupported Arrow type
/// (e.g. timestamp, binary, nested) is a hard error — Python's orjson path
/// likewise fails on a non-JSON-native column rather than emitting garbage.
fn arrow_column_to_json_values(array: &dyn Array) -> Result<Vec<Value>, SerializeError> {
    let n = array.len();

    // Downcast to a concrete primitive array and map each slot, honoring the
    // null mask. `Value::from` covers every integer width, f32/f64, bool, &str.
    macro_rules! map_native {
        ($arr_ty:ty) => {{
            let a = array
                .as_any()
                .downcast_ref::<$arr_ty>()
                .ok_or_else(|| -> SerializeError {
                    format!("arrow downcast to {} failed", stringify!($arr_ty)).into()
                })?;
            (0..n)
                .map(|i| {
                    if a.is_null(i) {
                        Value::Null
                    } else {
                        Value::from(a.value(i))
                    }
                })
                .collect()
        }};
    }

    let values: Vec<Value> = match array.data_type() {
        DataType::Null => vec![Value::Null; n],
        DataType::Boolean => map_native!(BooleanArray),
        DataType::Int8 => map_native!(Int8Array),
        DataType::Int16 => map_native!(Int16Array),
        DataType::Int32 => map_native!(Int32Array),
        DataType::Int64 => map_native!(Int64Array),
        DataType::UInt8 => map_native!(UInt8Array),
        DataType::UInt16 => map_native!(UInt16Array),
        DataType::UInt32 => map_native!(UInt32Array),
        DataType::UInt64 => map_native!(UInt64Array),
        DataType::Float32 => map_native!(Float32Array),
        DataType::Float64 => map_native!(Float64Array),
        DataType::Utf8 => map_native!(StringArray),
        DataType::LargeUtf8 => map_native!(LargeStringArray),
        other => {
            return Err(format!(
                "application/json table serializer does not support arrow column type {other:?}"
            )
            .into());
        }
    };
    Ok(values)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::registry::SerializationRegistry;

    /// Encode a single-batch RecordBatch into Arrow IPC bytes for the serializer.
    fn ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut w = FileWriter::try_new(&mut buf, &batch.schema()).unwrap();
            w.write(batch).unwrap();
            w.finish().unwrap();
        }
        buf
    }

    fn json_serializer() -> Arc<SerializerFn> {
        let reg = SerializationRegistry::new();
        register_table_serializers(&reg);
        reg.dispatch(StructureFamily::Table, mime::JSON)
            .expect("table application/json must be registered")
    }

    /// H1: `application/json` is registered for the table family.
    #[test]
    fn json_registered_for_table() {
        let reg = SerializationRegistry::new();
        register_table_serializers(&reg);
        assert!(
            reg.dispatch(StructureFamily::Table, mime::JSON).is_some(),
            "table application/json must be registered (Python table.py:141-147)"
        );
    }

    /// H1: output is a column-oriented dict `{col: [values...]}` and columns
    /// appear in schema order — NOT alphabetized. Column "b" precedes "a" in the
    /// schema, so it must precede "a" in the JSON (guards the serde_json
    /// `preserve_order` the column-dict relies on, matching `for column in df`).
    #[test]
    fn json_table_is_column_dict_in_schema_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int64, false),
            Field::new("a", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![4.0, 5.0, 6.0])),
            ],
        )
        .unwrap();
        let out = json_serializer()(&ipc_bytes(&batch), &serde_json::Value::Null).unwrap();
        let text = String::from_utf8(out.to_vec()).unwrap();

        // Column order preserved (schema order b, a), not sorted (a, b). Assert
        // on the raw bytes: re-parsing into a serde_json::Value would re-sort the
        // keys (no `preserve_order`), so only the wire bytes reveal the order.
        let pos_b = text.find(r#""b""#).expect("column b present");
        let pos_a = text.find(r#""a""#).expect("column a present");
        assert!(
            pos_b < pos_a,
            "columns must keep schema order (b before a): {text}"
        );

        // Values are independent of key order, so a re-parse is safe for them.
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["b"], serde_json::json!([1, 2, 3]));
        assert_eq!(parsed["a"], serde_json::json!([4.0, 5.0, 6.0]));
    }

    /// H1: an explicit Arrow null and a floating NaN both serialize to JSON
    /// `null` (Python `_series_to_json_safe`: missing → None, NaN → None).
    #[test]
    fn json_table_null_and_nan_become_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![
                Some(1.0),
                None,
                Some(f64::NAN),
            ]))],
        )
        .unwrap();
        let out = json_serializer()(&ipc_bytes(&batch), &serde_json::Value::Null).unwrap();
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["x"], serde_json::json!([1.0, null, null]));
    }

    /// H1: string and boolean columns map to their JSON-native forms.
    #[test]
    fn json_table_string_and_bool_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("flag", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["alpha", "beta"])),
                Arc::new(BooleanArray::from(vec![true, false])),
            ],
        )
        .unwrap();
        let out = json_serializer()(&ipc_bytes(&batch), &serde_json::Value::Null).unwrap();
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["s"], serde_json::json!(["alpha", "beta"]));
        assert_eq!(parsed["flag"], serde_json::json!([true, false]));
    }

    /// H1: an unsupported Arrow column type is a hard error (→ HTTP 500), never
    /// silently dropped or mis-encoded.
    #[test]
    fn json_table_unsupported_type_errors() {
        use arrow::array::TimestampMillisecondArray;
        use arrow::datatypes::TimeUnit;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(TimestampMillisecondArray::from(vec![0_i64, 1]))],
        )
        .unwrap();
        let err = json_serializer()(&ipc_bytes(&batch), &serde_json::Value::Null)
            .expect_err("unsupported column type must error");
        assert!(
            err.to_string().contains("does not support"),
            "error must name the unsupported-type cause: {err}"
        );
    }
}
