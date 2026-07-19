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

use crate::core::media_type::mime;
use crate::core::structures::StructureFamily;

use crate::serialization::registry::{SerializationRegistry, SerializeError, SerializerFn};

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

    // application/json-seq: row-oriented NDJSON, one JSON object per row
    // (table.py:158-173). Same orjson guard as above → register unconditionally.
    registry.register(
        StructureFamily::Table,
        mime::JSON_SEQ,
        json_seq_table_serializer(),
    );

    // text/html: render the table as an HTML <table> (Python `serialize_html`,
    // table.py:86-90, which calls `df.to_html`). Mirrors the array `text/html`
    // registration (array.rs) so browser navigation gets HTML, not a 406.
    registry.register(StructureFamily::Table, "text/html", html_table_serializer());
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

/// NDJSON serializer for the table family (Python `table.py:158-173`).
///
/// Despite the `application/json-seq` mimetype, Python tiled emits PLAIN
/// newline-delimited JSON: one `{column: value, ...}` object per row, joined by
/// `\n`, with no leading/trailing newline and no RFC 7464 RS framing (the
/// table.py comment calls it "the official mimetype for newline-delimited
/// JSON"). An empty table yields empty bytes. Row objects keep schema column
/// order, built by hand for the same `preserve_order` reason as the column-dict.
///
/// Note: the container `application/json-seq` serializer (`json_seq.rs`) uses
/// RS-framed RFC 7464 output instead — same mimetype, different format per
/// family — so this is a distinct registration, not shared code.
pub(crate) fn json_seq_table_serializer() -> SerializerFn {
    Box::new(move |data, _meta| -> Result<Bytes, SerializeError> {
        let columns = table_ipc_to_safe_columns(data)?;
        let nrows = columns.first().map(|(_, vals)| vals.len()).unwrap_or(0);
        let mut out = Vec::new();
        for row in 0..nrows {
            if row > 0 {
                out.push(b'\n');
            }
            out.push(b'{');
            for (idx, (name, vals)) in columns.iter().enumerate() {
                if idx > 0 {
                    out.push(b',');
                }
                serde_json::to_writer(&mut out, name).map_err(|e| format!("json encode: {e}"))?;
                out.push(b':');
                serde_json::to_writer(&mut out, &vals[row])
                    .map_err(|e| format!("json encode: {e}"))?;
            }
            out.push(b'}');
        }
        Ok(Bytes::from(out))
    })
}

/// HTML serializer for the table family (Python `serialize_html`,
/// table.py:86-90, which calls `df.to_html(index=preserve_index)` with
/// `preserve_index=False`).
///
/// Renders the Arrow IPC table as a minimal HTML `<table>`: a `<thead>` row of
/// column names followed by one `<tbody>` row per record. Cell values reuse the
/// JSON-safe column conversion (`table_ipc_to_safe_columns`), so missing/NaN
/// render as empty cells and numbers/bools/strings as their display text. Cell
/// and header text is HTML-escaped. The markup is NOT byte-identical to pandas
/// `to_html` (this workspace has no pandas), matching the array `text/html`
/// precedent (array.rs) of a functional, non-pandas HTML rendering.
///
/// Shared with the sparse family, which renders an HTML table from the same
/// Arrow IPC representation (Python sparse.py delegates to the table HTML
/// serializer after converting the COO array to a DataFrame).
pub(crate) fn html_table_serializer() -> SerializerFn {
    Box::new(move |data, _meta| -> Result<Bytes, SerializeError> {
        let columns = table_ipc_to_safe_columns(data)?;
        let nrows = columns.first().map(|(_, vals)| vals.len()).unwrap_or(0);
        let mut out = String::from(r#"<html><body><table border="1" class="dataframe">"#);
        out.push_str("<thead><tr>");
        for (name, _) in &columns {
            out.push_str("<th>");
            html_escape_into(&mut out, name);
            out.push_str("</th>");
        }
        out.push_str("</tr></thead><tbody>");
        for row in 0..nrows {
            out.push_str("<tr>");
            for (_, vals) in &columns {
                out.push_str("<td>");
                html_escape_into(&mut out, &html_cell_text(&vals[row]));
                out.push_str("</td>");
            }
            out.push_str("</tr>");
        }
        out.push_str("</tbody></table></body></html>");
        Ok(Bytes::from(out.into_bytes()))
    })
}

/// Display text for one JSON-safe cell value: a string verbatim, `null`
/// (missing/NaN) as an empty cell, and numbers/bools via their JSON repr.
fn html_cell_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Append `s` to `out`, escaping the HTML-significant characters so cell/header
/// text cannot break out of its element.
fn html_escape_into(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(c),
        }
    }
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
    // Materialize every batch up front so the int+null→float64 promotion below is
    // a table-wide decision: a nullable integer column split across batches must
    // format uniformly (a null in ANY batch promotes the whole column), matching
    // pandas, where `to_pandas()` promotes the column once at read.
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("ipc batch: {e}"))?;
    // Upstream serializes tables from `adapter.read()` = pyarrow `to_pandas()`
    // (adapters/arrow.py:292), which promotes a nullable integer column with ANY
    // null to float64/NaN before `_series_to_json_safe` runs (serialization/
    // table.py:113-136). The CSV path mirrors this via `ColPlan::IntToFloat`
    // (csv_table.rs); do the same here so json / json-seq / html emit floats, not
    // ints, for such a column. A fully-populated integer column stays integer.
    let promote_int_to_float: Vec<bool> = (0..schema.fields().len())
        .map(|c| {
            is_integer_type(schema.field(c).data_type())
                && batches.iter().any(|b| b.column(c).null_count() > 0)
        })
        .collect();
    let mut columns: Vec<(String, Vec<Value>)> = schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), Vec::new()))
        .collect();
    for batch in &batches {
        for (col_idx, column) in columns.iter_mut().enumerate() {
            let vals = arrow_column_to_json_values(
                batch.column(col_idx).as_ref(),
                promote_int_to_float[col_idx],
            )?;
            column.1.extend(vals);
        }
    }
    Ok(columns)
}

/// The Arrow integer types pandas promotes to float64 when the column contains a
/// null (int-with-missing → float64 at read; mirrors `csv_table.rs`).
fn is_integer_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

/// Convert one Arrow array into a list of JSON-safe values (one per row).
///
/// Numeric/boolean/UTF-8 columns map to their JSON-native forms; `serde_json`'s
/// `From<f32>`/`From<f64>` map NaN and infinities to `Value::Null`, matching
/// Python's NaN→`None` rule, and an explicit Arrow null at any index is also
/// `null`. Temporal columns (timestamp/date/time) are rendered as ISO-8601
/// strings — Python's `_series_to_json_safe` converts `pandas.Timestamp` via
/// `.isoformat()` (table.py:135-139) and orjson renders date/time natively, so
/// a parquet table with a timestamp column serializes rather than 500ing.
/// A genuinely non-JSON-native column (binary, nested) remains a hard error,
/// matching Python's orjson failure on such a column rather than emitting
/// garbage.
fn arrow_column_to_json_values(
    array: &dyn Array,
    promote_int_to_float: bool,
) -> Result<Vec<Value>, SerializeError> {
    let n = array.len();

    // pandas int-with-missing → float64 (decided table-wide in
    // `table_ipc_to_safe_columns`): cast the nullable integer column to Float64
    // and fall through the Float64 arm so values become floats (`1`→`1.0`) and
    // nulls stay null — matching upstream `to_pandas()` + `_series_to_json_safe`.
    if promote_int_to_float {
        let casted = arrow::compute::cast(array, &DataType::Float64)
            .map_err(|e| -> SerializeError { format!("int->float64 cast: {e}").into() })?;
        return arrow_column_to_json_values(casted.as_ref(), false);
    }

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
        // Temporal columns → ISO-8601 strings. Python's `_series_to_json_safe`
        // converts `pandas.Timestamp` with `.isoformat()` (table.py:135-139) and
        // orjson renders date/time natively; Arrow's cast-to-Utf8 yields the
        // equivalent ISO-8601 text and handles every unit/timezone uniformly.
        // Null slots stay JSON null, matching the null mask of every other arm.
        DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_) => {
            let cast =
                arrow::compute::cast(array, &DataType::Utf8).map_err(|e| -> SerializeError {
                    format!("arrow temporal column cast to string failed: {e}").into()
                })?;
            let s =
                cast.as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| -> SerializeError {
                        "arrow temporal cast did not yield a Utf8 array".into()
                    })?;
            (0..n)
                .map(|i| {
                    if s.is_null(i) {
                        Value::Null
                    } else {
                        Value::from(s.value(i))
                    }
                })
                .collect()
        }
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
    use crate::serialization::registry::SerializationRegistry;

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

    /// Finding 1: a nullable integer column with ≥1 null promotes to float64/NaN
    /// in JSON, mirroring upstream `to_pandas()` (adapters/arrow.py:292) +
    /// `_series_to_json_safe` (table.py:113-136). Before this fix the JSON path
    /// emitted the raw ints `[1,null,2]`; upstream emits `[1.0,null,2.0]`. A
    /// serde_json `Number` int and float are NOT equal, so this pins the float-ness.
    #[test]
    fn json_table_int_column_with_null_promotes_to_float() {
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(2)]))],
        )
        .unwrap();
        let out = json_serializer()(&ipc_bytes(&batch), &serde_json::Value::Null).unwrap();
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["b"], serde_json::json!([1.0, null, 2.0]));
        let text = String::from_utf8(out.to_vec()).unwrap();
        assert!(
            text.contains("1.0") && text.contains("2.0"),
            "promoted column must render floats, got {text}"
        );
    }

    /// Finding 1 (json-seq): the same int+null→float promotion drives the
    /// row-oriented NDJSON path (shared `table_ipc_to_safe_columns`).
    #[test]
    fn json_seq_table_int_column_with_null_promotes_to_float() {
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(2)]))],
        )
        .unwrap();
        let out = json_seq_serializer()(&ipc_bytes(&batch), &serde_json::Value::Null).unwrap();
        let text = String::from_utf8(out.to_vec()).unwrap();
        assert_eq!(
            text, "{\"b\":1.0}\n{\"b\":null}\n{\"b\":2.0}",
            "int+null column must promote to float in json-seq rows"
        );
    }

    /// Finding 1 (html): the int+null→float promotion also drives HTML cell text
    /// (shared `table_ipc_to_safe_columns`), so a promoted cell renders `1.0`.
    #[test]
    fn html_table_int_column_with_null_promotes_to_float() {
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(2)]))],
        )
        .unwrap();
        let out = html_serializer()(&ipc_bytes(&batch), &serde_json::Value::Null).unwrap();
        let html = String::from_utf8(out.to_vec()).unwrap();
        assert!(html.contains("<td>1.0</td>"), "promoted cell 1.0: {html}");
        assert!(html.contains("<td>2.0</td>"), "promoted cell 2.0: {html}");
        assert!(html.contains("<td></td>"), "null cell empty: {html}");
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

    fn json_seq_serializer() -> Arc<SerializerFn> {
        let reg = SerializationRegistry::new();
        register_table_serializers(&reg);
        reg.dispatch(StructureFamily::Table, mime::JSON_SEQ)
            .expect("table application/json-seq must be registered")
    }

    /// H2: `application/json-seq` is registered for the table family.
    #[test]
    fn json_seq_registered_for_table() {
        let reg = SerializationRegistry::new();
        register_table_serializers(&reg);
        assert!(
            reg.dispatch(StructureFamily::Table, mime::JSON_SEQ)
                .is_some(),
            "table application/json-seq must be registered (Python table.py:158-173)"
        );
    }

    /// H2: output is plain newline-delimited JSON — one `{col: value}` object
    /// per row in schema order, joined by `\n`, no RFC 7464 RS framing, no
    /// trailing newline (Python `json_sequence`, table.py:162-173).
    #[test]
    fn json_seq_table_emits_ndjson_rows() {
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
        let out = json_seq_serializer()(&ipc_bytes(&batch), &serde_json::Value::Null).unwrap();
        let text = String::from_utf8(out.to_vec()).unwrap();
        assert_eq!(
            text, "{\"b\":1,\"a\":4.0}\n{\"b\":2,\"a\":5.0}\n{\"b\":3,\"a\":6.0}",
            "rows must be NDJSON in schema column order, no RS framing, no trailing newline"
        );
        // No record-separator byte (0x1E) — distinguishes from the container
        // RFC 7464 serializer.
        assert!(
            !out.contains(&0x1E),
            "table json-seq must not use RS framing"
        );
    }

    /// H2: an empty table serializes to empty bytes (Python `n == 0: yield b""`).
    #[test]
    fn json_seq_table_empty_is_empty_bytes() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap();
        let out = json_seq_serializer()(&ipc_bytes(&batch), &serde_json::Value::Null).unwrap();
        assert!(out.is_empty(), "empty table must yield empty json-seq body");
    }

    /// #1378: a single-row table is the boundary the `row > 0` newline guard
    /// exists for — it must emit exactly one JSON object with NO leading or
    /// trailing newline (not the empty-table case, and not a two-row case that
    /// would mask an off-by-one in the guard). Mirrors Python's `n == 1` path
    /// through `json_sequence` (table.py: first row has no leading newline,
    /// loop over `range(1, n)` never runs).
    #[test]
    fn json_seq_table_single_row_has_no_newline_framing() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int64, false),
            Field::new("a", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Float64Array::from(vec![4.0])),
            ],
        )
        .unwrap();
        let out = json_seq_serializer()(&ipc_bytes(&batch), &serde_json::Value::Null).unwrap();
        let text = String::from_utf8(out.to_vec()).unwrap();
        assert_eq!(
            text, "{\"b\":1,\"a\":4.0}",
            "single row must be exactly one JSON object, no newline framing"
        );
        assert!(
            !text.starts_with('\n') && !text.ends_with('\n'),
            "single row must carry no leading/trailing newline, got {text:?}"
        );
        assert_eq!(
            text.matches('\n').count(),
            0,
            "single row must contain no newline at all, got {text:?}"
        );
    }

    /// A genuinely non-JSON-native Arrow column type (binary) is a hard error
    /// (→ HTTP 500), never silently dropped or mis-encoded — matching orjson's
    /// failure on a bytes column rather than emitting garbage.
    #[test]
    fn json_table_unsupported_type_errors() {
        use arrow::array::BinaryArray;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "blob",
            DataType::Binary,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BinaryArray::from(vec![
                b"\x00\x01".as_ref(),
                b"\x02".as_ref(),
            ]))],
        )
        .unwrap();
        let err = json_serializer()(&ipc_bytes(&batch), &serde_json::Value::Null)
            .expect_err("unsupported column type must error");
        assert!(
            err.to_string().contains("does not support"),
            "error must name the unsupported-type cause: {err}"
        );
    }

    /// A temporal column (timestamp/date) serializes to ISO-8601 strings rather
    /// than 500ing: Python's `_series_to_json_safe` converts `pandas.Timestamp`
    /// via `.isoformat()` (table.py:135-139). Reachable through ParquetAdapter,
    /// which passes the native Arrow schema through, so a parquet table with a
    /// timestamp column must serialize. Null slots stay JSON null.
    #[test]
    fn json_table_temporal_columns_become_iso_strings() {
        use arrow::array::{Date32Array, TimestampMillisecondArray};
        use arrow::datatypes::TimeUnit;
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("d", DataType::Date32, true),
        ]));
        // ts: 2021-01-01T00:00:00 UTC (1609459200000 ms), then null.
        // d:  2021-01-01 (18628 days since epoch), then null.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(1_609_459_200_000_i64),
                    None,
                ])),
                Arc::new(Date32Array::from(vec![Some(18_628_i32), None])),
            ],
        )
        .unwrap();
        let out = json_serializer()(&ipc_bytes(&batch), &serde_json::Value::Null).unwrap();
        let parsed: Value = serde_json::from_slice(&out).unwrap();

        // First slot is an ISO-8601 string with the right calendar date/time;
        // second slot is JSON null. Assert on a date/time prefix rather than the
        // exact fractional-second spelling so the test is robust to Arrow's cast
        // formatting while still proving the calendar value is correct.
        let ts0 = parsed["ts"][0].as_str().expect("ts[0] must be a string");
        assert!(
            ts0.starts_with("2021-01-01T00:00:00"),
            "timestamp must render as ISO-8601: {ts0}"
        );
        assert_eq!(parsed["ts"][1], Value::Null, "null timestamp stays null");

        let d0 = parsed["d"][0].as_str().expect("d[0] must be a string");
        assert!(
            d0.starts_with("2021-01-01"),
            "date must render as ISO-8601: {d0}"
        );
        assert_eq!(parsed["d"][1], Value::Null, "null date stays null");
    }

    fn html_serializer() -> Arc<SerializerFn> {
        let reg = SerializationRegistry::new();
        register_table_serializers(&reg);
        reg.dispatch(StructureFamily::Table, "text/html")
            .expect("table text/html must be registered")
    }

    /// M2: `text/html` is registered for the table family (Python serialize_html,
    /// table.py:86-90) so `Accept: text/html` gets an HTML table, not a 406.
    #[test]
    fn html_registered_for_table() {
        let reg = SerializationRegistry::new();
        register_table_serializers(&reg);
        assert!(
            reg.dispatch(StructureFamily::Table, "text/html").is_some(),
            "table text/html must be registered (Python table.py:86-90)"
        );
    }

    /// M2: the HTML body is an escaped `<table>` with a header row in schema
    /// column order and one row per record; a string containing `<` is escaped
    /// so it cannot break out of its cell.
    #[test]
    fn html_table_renders_escaped_table_in_schema_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int64, false),
            Field::new("a", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "a<b"])),
            ],
        )
        .unwrap();
        let out = html_serializer()(&ipc_bytes(&batch), &serde_json::Value::Null).unwrap();
        let html = String::from_utf8(out.to_vec()).unwrap();

        assert!(
            html.starts_with(r#"<html><body><table border="1" class="dataframe">"#),
            "must open an html table: {html}"
        );
        assert!(
            html.ends_with("</table></body></html>"),
            "must close: {html}"
        );
        // Headers present in schema order (b before a).
        let pos_b = html.find("<th>b</th>").expect("header b present");
        let pos_a = html.find("<th>a</th>").expect("header a present");
        assert!(pos_b < pos_a, "headers must keep schema order: {html}");
        // Data cells rendered.
        assert!(html.contains("<td>1</td>"), "int cell rendered: {html}");
        assert!(html.contains("<td>x</td>"), "string cell rendered: {html}");
        // `<` inside a string is escaped, not emitted raw.
        assert!(
            html.contains("<td>a&lt;b</td>"),
            "string cell must be HTML-escaped: {html}"
        );
    }

    /// M2: a null/NaN cell renders as an empty `<td>`, not the text "null".
    #[test]
    fn html_table_null_cell_is_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![Some(1.0), None]))],
        )
        .unwrap();
        let out = html_serializer()(&ipc_bytes(&batch), &serde_json::Value::Null).unwrap();
        let html = String::from_utf8(out.to_vec()).unwrap();
        assert!(
            html.contains("<td>1.0</td>"),
            "present value rendered: {html}"
        );
        assert!(
            html.contains("<td></td>"),
            "missing value must be an empty cell: {html}"
        );
        assert!(
            !html.contains("null"),
            "must not emit the text 'null': {html}"
        );
    }
}
