//! CSV serializer for `table` family.
//!
//! Re-serves the Arrow IPC bytes produced by the table partition handler as
//! CSV, matching upstream `serialize_csv` (`tiled/serialization/table.py:57-62`),
//! which formats the frame with pandas `DataFrame.to_csv(index=False)`. Arrow's
//! CSV writer diverges from pandas on several column kinds, so one normalization
//! pass rewrites each batch to the pandas-equivalent representation *by
//! construction* before the writer runs ([`normalize_batches_for_pandas_csv`]):
//!
//! - **Boolean** → `True`/`False` (arrow writes lowercase `true`/`false`).
//! - **Integer column with any null** → `Float64`, so `5` prints as `5.0`
//!   (pandas promotes an int column with missing values to float64 at read).
//!   A fully-populated integer column stays integer. The null scan spans *all*
//!   batches so one column's formatting is uniform across partitions — which
//!   requires materialising the batches instead of streaming them.
//! - **Float `NaN` value** → null, so it prints as the empty NA token like
//!   pandas (`na_rep=""`); arrow otherwise prints the literal `NaN`. `±inf`
//!   is preserved (pandas and arrow both print `inf`/`-inf`).
//!
//! - **Naive Timestamp / Date / Time** → the pandas string form, which differs
//!   from arrow's RFC3339 (arrow uses a `T` separator and always emits the
//!   resolution's full fractional). NaT/null prints as the empty NA token like
//!   every other column. All rules below were verified against a pandas 3.0.3 +
//!   pyarrow 25.0.0 oracle running the exact upstream pipeline (`arrow-IPC →
//!   read_pandas → df.to_csv(index=False)`):
//!   - separator is a space, e.g. `2021-06-15 13:45:30`;
//!   - a Timestamp column whose values are **all** at midnight prints
//!     date-only `YYYY-MM-DD` (pandas `is_dates_only`); any non-midnight value
//!     makes the whole column print the full datetime;
//!   - fractional seconds are uniform across the *timestamp* column at the
//!     smallest of `{3, 6, 9}` digits that captures the finest sub-second
//!     present (none if all whole-second) — NaT is ignored when deciding both
//!     rules;
//!   - Date32/Date64 → `YYYY-MM-DD`. Time32/Time64 → `HH:MM:SS[.ffffff]`:
//!     pyarrow maps Time to a pandas *object* column of `datetime.time`, so —
//!     unlike Timestamp — its fraction is decided *per element*
//!     (`datetime.time.isoformat`: six digits when microseconds are present,
//!     none otherwise), never the uniform `{3,6,9}` timestamp width.
//!
//! Booleans-as-`True`/`False`, `5.0`, formatted datetimes, and empty NA all
//! pass through the writer verbatim (no delimiter/quote/newline). A lone empty
//! NA field is quoted `""` by the writer, matching pandas' Python-`csv` writer.
//!
//! Residual divergence (documented, not fixed): **timezone-aware** Timestamp
//! columns retain arrow's RFC3339 form. Reproducing pandas' `+HH:MM` wall-clock
//! for a named IANA zone needs a timezone database (`chrono-tz`, not a
//! dependency here); no tiled-rs adapter emits a tz-aware column, so this is
//! reachable only via a passed-through Arrow/Parquet file.

#![cfg(feature = "csv")]

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    StringArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::csv::writer::WriterBuilder;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::FileReader;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use bytes::Bytes;
use chrono::{NaiveDateTime, NaiveTime, Timelike};

use crate::core::media_type::mime;
use crate::core::structures::StructureFamily;

use crate::serialization::registry::{SerializationRegistry, SerializeError, SerializerFn};

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
    Box::new(move |data, _meta| -> Result<Bytes, SerializeError> {
        let cursor = Cursor::new(data.to_vec());
        let reader = FileReader::try_new(cursor, None).map_err(|e| format!("ipc reader: {e}"))?;
        let schema = reader.schema();
        // Materialise the batches: the int-null scan (see the module docs) must
        // span the whole table to keep a column's formatting uniform.
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch.map_err(|e| format!("ipc batch: {e}"))?);
        }
        let (_out_schema, out_batches) = normalize_batches_for_pandas_csv(&schema, &batches)?;

        let mut buf = Vec::new();
        {
            let mut writer = WriterBuilder::new()
                .with_header(with_header)
                .build(&mut buf);
            for batch in &out_batches {
                writer.write(batch).map_err(|e| format!("csv write: {e}"))?;
            }
        }
        Ok(Bytes::from(buf))
    })
}

/// True for the numpy-integer arrow types that pandas promotes to float64 when
/// the column carries a missing value.
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

/// Per-column rewrite decision, computed once over the whole table so a column
/// split across several batches formats uniformly.
enum ColPlan {
    /// Leave unchanged — includes tz-aware Timestamp (see the module docs).
    Passthrough,
    /// Boolean -> Utf8 `True`/`False`.
    Bool,
    /// Integer-with-nulls -> Float64.
    IntToFloat,
    /// Float column: replace NaN values with null.
    FloatDenan,
    /// Naive Timestamp -> Utf8; `dates_only`/`frac` are the table-wide rules.
    Timestamp {
        unit: TimeUnit,
        dates_only: bool,
        frac: u8,
    },
    /// Date32/Date64 -> Utf8 `YYYY-MM-DD`.
    Date,
    /// Time32/Time64 -> Utf8 `HH:MM:SS[.ffffff]` (per-element, `datetime.time`).
    Time { unit: TimeUnit },
}

/// The arrow type a plan's output column carries.
fn plan_output_type(plan: &ColPlan, original: &DataType) -> DataType {
    match plan {
        ColPlan::Bool | ColPlan::Timestamp { .. } | ColPlan::Date | ColPlan::Time { .. } => {
            DataType::Utf8
        }
        ColPlan::IntToFloat => DataType::Float64,
        ColPlan::Passthrough | ColPlan::FloatDenan => original.clone(),
    }
}

/// Per-index naive `NaiveDateTime`s for a naive Timestamp column (nulls -> None).
fn timestamp_datetimes(col: &ArrayRef, unit: TimeUnit) -> Vec<Option<NaiveDateTime>> {
    macro_rules! collect {
        ($arr:ty) => {{
            let a = col
                .as_any()
                .downcast_ref::<$arr>()
                .expect("timestamp array");
            (0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        None
                    } else {
                        a.value_as_datetime(i)
                    }
                })
                .collect()
        }};
    }
    match unit {
        TimeUnit::Second => collect!(TimestampSecondArray),
        TimeUnit::Millisecond => collect!(TimestampMillisecondArray),
        TimeUnit::Microsecond => collect!(TimestampMicrosecondArray),
        TimeUnit::Nanosecond => collect!(TimestampNanosecondArray),
    }
}

/// Per-index `NaiveTime`s for a Time32/Time64 column (nulls -> None).
fn time_values(col: &ArrayRef, unit: TimeUnit) -> Vec<Option<NaiveTime>> {
    macro_rules! collect {
        ($arr:ty) => {{
            let a = col.as_any().downcast_ref::<$arr>().expect("time array");
            (0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        None
                    } else {
                        a.value_as_time(i)
                    }
                })
                .collect()
        }};
    }
    match unit {
        TimeUnit::Second => collect!(Time32SecondArray),
        TimeUnit::Millisecond => collect!(Time32MillisecondArray),
        TimeUnit::Microsecond => collect!(Time64MicrosecondArray),
        TimeUnit::Nanosecond => collect!(Time64NanosecondArray),
    }
}

/// Smallest of `{0,3,6,9}` fractional digits that represents every value's
/// sub-second part exactly (0 = all whole-second), matching pandas' per-column
/// uniform fractional width.
fn frac_digits<I: IntoIterator<Item = u32>>(subsecond_nanos: I) -> u8 {
    let (mut any, mut ms_ok, mut us_ok) = (false, true, true);
    for ns in subsecond_nanos {
        if ns != 0 {
            any = true;
        }
        if ns % 1_000_000 != 0 {
            ms_ok = false;
        }
        if ns % 1_000 != 0 {
            us_ok = false;
        }
    }
    if !any {
        0
    } else if ms_ok {
        3
    } else if us_ok {
        6
    } else {
        9
    }
}

/// Append `.fff…` (`frac` fixed digits) for a sub-second `nanos` value.
fn push_fraction(s: &mut String, nanos: u32, frac: u8) {
    match frac {
        0 => {}
        3 => s.push_str(&format!(".{:03}", nanos / 1_000_000)),
        6 => s.push_str(&format!(".{:06}", nanos / 1_000)),
        _ => s.push_str(&format!(".{nanos:09}")),
    }
}

/// pandas `str` of a naive datetime: `YYYY-MM-DD` when the whole column is
/// midnight, else `YYYY-MM-DD HH:MM:SS[.fff]`.
fn fmt_datetime(dt: &NaiveDateTime, dates_only: bool, frac: u8) -> String {
    if dates_only {
        dt.format("%Y-%m-%d").to_string()
    } else {
        let mut s = dt.format("%Y-%m-%d %H:%M:%S").to_string();
        push_fraction(&mut s, dt.time().nanosecond(), frac);
        s
    }
}

/// pandas `str` of a time: matches `datetime.time.isoformat()` — `HH:MM:SS`,
/// with `.ffffff` (six digits) only when the microsecond part is non-zero.
/// pyarrow maps Time to a pandas *object* column of `datetime.time`, so — unlike
/// naive Timestamp — the width is per-element and never 3 or 9 digits. Time
/// resolutions finer than microseconds (`time64[ns]` with non-zero nanoseconds)
/// have no `datetime.time`/pandas representation at all (upstream raises); we
/// truncate to microseconds rather than error.
fn fmt_time(t: &NaiveTime) -> String {
    let us = t.nanosecond() / 1_000;
    if us == 0 {
        t.format("%H:%M:%S").to_string()
    } else {
        format!("{}.{:06}", t.format("%H:%M:%S"), us)
    }
}

/// Decide the rewrite plan for one column by scanning every batch.
fn plan_column(dt: &DataType, col: usize, batches: &[RecordBatch]) -> ColPlan {
    match dt {
        DataType::Boolean => ColPlan::Bool,
        t if is_integer_type(t) => {
            // pandas int-with-missing -> float64 at read; scan the whole table.
            if batches.iter().any(|b| b.column(col).null_count() > 0) {
                ColPlan::IntToFloat
            } else {
                ColPlan::Passthrough
            }
        }
        DataType::Float32 | DataType::Float64 => ColPlan::FloatDenan,
        DataType::Timestamp(unit, None) => {
            let dts: Vec<Option<NaiveDateTime>> = batches
                .iter()
                .flat_map(|b| timestamp_datetimes(b.column(col), *unit))
                .collect();
            // dates_only iff there is at least one value and every non-null value
            // is exactly midnight (NaT ignored).
            let mut saw = false;
            let mut all_midnight = true;
            for d in dts.iter().flatten() {
                saw = true;
                let t = d.time();
                if t.num_seconds_from_midnight() != 0 || t.nanosecond() != 0 {
                    all_midnight = false;
                    break;
                }
            }
            let frac = frac_digits(dts.iter().flatten().map(|d| d.time().nanosecond()));
            ColPlan::Timestamp {
                unit: *unit,
                dates_only: saw && all_midnight,
                frac,
            }
        }
        // tz-aware Timestamp: documented residual divergence — left to arrow.
        DataType::Timestamp(_, Some(_)) => ColPlan::Passthrough,
        DataType::Date32 | DataType::Date64 => ColPlan::Date,
        // Time formats per-element (object column of `datetime.time`), so no
        // whole-column frac scan — unlike naive Timestamp.
        DataType::Time32(unit) | DataType::Time64(unit) => ColPlan::Time { unit: *unit },
        _ => ColPlan::Passthrough,
    }
}

/// Rewrite every record batch so arrow's CSV writer reproduces pandas
/// `DataFrame.to_csv(index=False)` output by construction (see the module docs).
/// Plans are decided by scanning *all* batches, so the whole table is
/// materialised before writing.
fn normalize_batches_for_pandas_csv(
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<(SchemaRef, Vec<RecordBatch>), SerializeError> {
    let ncols = schema.fields().len();
    let plans: Vec<ColPlan> = (0..ncols)
        .map(|c| plan_column(schema.field(c).data_type(), c, batches))
        .collect();

    let out_fields: Vec<Field> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(c, f)| {
            Field::new(
                f.name(),
                plan_output_type(&plans[c], f.data_type()),
                f.is_nullable(),
            )
        })
        .collect();
    let out_schema: SchemaRef = Arc::new(Schema::new(out_fields));

    let mut out_batches = Vec::with_capacity(batches.len());
    for b in batches {
        let cols: Result<Vec<ArrayRef>, SerializeError> = (0..ncols)
            .map(|c| apply_plan(b.column(c), &plans[c]))
            .collect();
        // Carry the source batch's row count onto the rebuilt batch. For a
        // populated batch this equals every column's length (unchanged
        // behaviour); for a zero-column batch (an empty dataset) it is what lets
        // arrow build a zero-row batch at all — `try_new` cannot infer a row
        // count from zero columns and fails with "must either specify a row
        // count or at least one column", surfacing as a 500 on an empty export.
        let options = RecordBatchOptions::new().with_row_count(Some(b.num_rows()));
        let rb = RecordBatch::try_new_with_options(out_schema.clone(), cols?, &options)
            .map_err(|e| -> SerializeError { format!("csv normalize batch: {e}").into() })?;
        out_batches.push(rb);
    }
    Ok((out_schema, out_batches))
}

/// Apply a column's [`ColPlan`], producing the rewritten array.
fn apply_plan(col: &ArrayRef, plan: &ColPlan) -> Result<ArrayRef, SerializeError> {
    match plan {
        ColPlan::Passthrough => Ok(col.clone()),
        ColPlan::Bool => {
            let a = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| -> SerializeError { "expected BooleanArray".into() })?;
            // pandas writes the Python bool str ("True"/"False"); nulls -> NA "".
            let out: StringArray = (0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        None
                    } else if a.value(i) {
                        Some("True")
                    } else {
                        Some("False")
                    }
                })
                .collect();
            Ok(Arc::new(out))
        }
        ColPlan::IntToFloat => arrow::compute::cast(col, &DataType::Float64)
            .map_err(|e| -> SerializeError { format!("csv int->float64 cast: {e}").into() }),
        // Float NaN -> null so the writer emits the empty NA token like pandas;
        // ±inf is left intact (both pandas and arrow print inf/-inf).
        ColPlan::FloatDenan => denan_float(col),
        ColPlan::Timestamp {
            unit,
            dates_only,
            frac,
        } => {
            let out: StringArray = timestamp_datetimes(col, *unit)
                .into_iter()
                .map(|d| d.map(|dt| fmt_datetime(&dt, *dates_only, *frac)))
                .collect();
            Ok(Arc::new(out))
        }
        ColPlan::Date => date_strings(col),
        ColPlan::Time { unit } => {
            let out: StringArray = time_values(col, *unit)
                .into_iter()
                .map(|t| t.map(|t| fmt_time(&t)))
                .collect();
            Ok(Arc::new(out))
        }
    }
}

/// Replace NaN float values with null (preserving existing nulls and ±inf).
fn denan_float(col: &ArrayRef) -> Result<ArrayRef, SerializeError> {
    match col.data_type() {
        DataType::Float32 => {
            let a = col
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| -> SerializeError { "expected Float32Array".into() })?;
            let out: Float32Array = (0..a.len())
                .map(|i| {
                    if a.is_null(i) || a.value(i).is_nan() {
                        None
                    } else {
                        Some(a.value(i))
                    }
                })
                .collect();
            Ok(Arc::new(out))
        }
        DataType::Float64 => {
            let a = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| -> SerializeError { "expected Float64Array".into() })?;
            let out: Float64Array = (0..a.len())
                .map(|i| {
                    if a.is_null(i) || a.value(i).is_nan() {
                        None
                    } else {
                        Some(a.value(i))
                    }
                })
                .collect();
            Ok(Arc::new(out))
        }
        _ => Ok(col.clone()),
    }
}

/// Date32/Date64 -> Utf8 `YYYY-MM-DD` (nulls -> None).
fn date_strings(col: &ArrayRef) -> Result<ArrayRef, SerializeError> {
    macro_rules! collect {
        ($arr:ty) => {{
            let a = col
                .as_any()
                .downcast_ref::<$arr>()
                .ok_or_else(|| -> SerializeError { "expected date array".into() })?;
            let out: StringArray = (0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        None
                    } else {
                        a.value_as_date(i).map(|d| d.format("%Y-%m-%d").to_string())
                    }
                })
                .collect();
            Ok(Arc::new(out))
        }};
    }
    match col.data_type() {
        DataType::Date32 => collect!(Date32Array),
        DataType::Date64 => collect!(Date64Array),
        _ => Err("expected a date column".into()),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use super::*;
    use crate::serialization::registry::SerializationRegistry;

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

    /// Wave-21 Candidate A: table CSV must match pandas `df.to_csv(index=False)`
    /// (`tiled/serialization/table.py:57-62`) for bool / nullable-int / float
    /// columns. Each asserted value cites the pandas rule it reproduces.
    #[test]
    fn csv_matches_pandas_to_csv_for_bool_int_float() {
        use arrow::array::{BooleanArray, Float64Array, Int64Array};

        let schema = Arc::new(Schema::new(vec![
            Field::new("flag", DataType::Boolean, true),
            // Nullable int -> pandas promotes int-with-missing to float64 at read.
            Field::new("ni", DataType::Int64, true),
            // Fully-populated int -> stays integer.
            Field::new("fi", DataType::Int64, false),
            Field::new("val", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                Arc::new(Int64Array::from(vec![Some(10), None, Some(30)])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                // row1 carries a NaN *value* (not null); row2 is null.
                Arc::new(Float64Array::from(vec![Some(1.5), Some(f64::NAN), None])),
            ],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut w = FileWriter::try_new(&mut ipc, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }

        let reg = table_registry();
        let ser = reg
            .dispatch(StructureFamily::Table, mime::CSV)
            .expect("text/csv registered");
        let out = ser(&ipc, &serde_json::Value::Null).unwrap();
        let text = String::from_utf8(out.to_vec()).unwrap();

        // Exact pandas `df.to_csv(index=False)` bytes:
        //  - bool  -> "True"/"False"  (pandas writes Python bool str; arrow lowercases)
        //  - ni    -> "10.0"/""/"30.0" (int+missing promoted to float64)
        //  - fi    -> "1"/"2"/"3"       (fully-populated int stays int)
        //  - val   -> "1.5"/""/""       (float NaN and None both -> na_rep "")
        let expected = "flag,ni,fi,val\nTrue,10.0,1,1.5\nFalse,,2,\n,30.0,3,\n";
        assert_eq!(text, expected, "table CSV must match pandas df.to_csv");

        // Per-divergence pins:
        assert!(
            text.contains("True") && text.contains("False") && !text.contains("true"),
            "booleans must be Title-case True/False, not arrow lowercase: {text:?}"
        );
        assert!(
            text.contains("10.0") && text.contains("30.0"),
            "nullable int column must promote to float64: {text:?}"
        );
        assert!(
            !text.contains("NaN"),
            "float NaN must render as empty NA token, not literal NaN: {text:?}"
        );
        assert!(
            text.contains("True,10.0,1,1.5"),
            "fully-populated int column must stay integer (fi=1, not 1.0): {text:?}"
        );
    }

    /// Serialize a single-column table through the registered `text/csv`
    /// serializer and return the CSV text.
    fn single_col_csv(field: Field, col: ArrayRef) -> String {
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();
        let mut ipc = Vec::new();
        {
            let mut w = FileWriter::try_new(&mut ipc, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        let reg = table_registry();
        let ser = reg
            .dispatch(StructureFamily::Table, mime::CSV)
            .expect("text/csv registered");
        let out = ser(&ipc, &serde_json::Value::Null).unwrap();
        String::from_utf8(out.to_vec()).unwrap()
    }

    /// Wave-21 Candidate A: naive Timestamp / Date / Time columns must match
    /// pandas `df.to_csv(index=False)`. Every expected string below is the exact
    /// byte output of the upstream pipeline (`arrow-IPC → to_pandas →
    /// df.to_csv`) captured from a pandas 3.0.3 + pyarrow 25.0.0 oracle; each
    /// case names the pandas rule it pins.
    #[test]
    fn csv_matches_pandas_to_csv_for_temporal() {
        use arrow::array::{
            Date32Array, Date64Array, Time32MillisecondArray, Time32SecondArray,
            Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
        };
        use chrono::NaiveDate;

        // Helpers: build the arrow integer payloads via chrono so the intent is
        // legible and epoch arithmetic is not hand-rolled.
        let us = |y, mo, d, h, mi, s, micro: u32| -> i64 {
            NaiveDate::from_ymd_opt(y, mo, d)
                .unwrap()
                .and_hms_micro_opt(h, mi, s, micro)
                .unwrap()
                .and_utc()
                .timestamp_micros()
        };
        let ns = |y, mo, d, h, mi, s, nano: u32| -> i64 {
            NaiveDate::from_ymd_opt(y, mo, d)
                .unwrap()
                .and_hms_nano_opt(h, mi, s, nano)
                .unwrap()
                .and_utc()
                .timestamp_nanos_opt()
                .unwrap()
        };
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let d32 = |y, mo, d| (NaiveDate::from_ymd_opt(y, mo, d).unwrap() - epoch).num_days() as i32;
        let d64 = |y, mo, d| {
            NaiveDate::from_ymd_opt(y, mo, d)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_millis()
        };
        let t32s = |h: i32, mi: i32, s: i32| h * 3600 + mi * 60 + s;
        let t32ms = |h: i32, mi: i32, s: i32, ms: i32| (h * 3600 + mi * 60 + s) * 1000 + ms;
        let t64us =
            |h: i64, mi: i64, s: i64, micro: i64| (h * 3600 + mi * 60 + s) * 1_000_000 + micro;

        let tsus = |name: &str, v: Vec<Option<i64>>| {
            single_col_csv(
                Field::new(name, DataType::Timestamp(TimeUnit::Microsecond, None), true),
                Arc::new(TimestampMicrosecondArray::from(v)),
            )
        };

        // Timestamp, whole-second, non-midnight present, with a NaT and a
        // midnight value -> full datetime, space separator, no fraction.
        assert_eq!(
            tsus(
                "t",
                vec![
                    Some(us(2021, 6, 15, 13, 45, 30, 0)),
                    None,
                    Some(us(2021, 6, 15, 0, 0, 0, 0)),
                ],
            ),
            "t\n2021-06-15 13:45:30\n\"\"\n2021-06-15 00:00:00\n",
        );

        // is_dates_only: every non-null value at midnight -> date-only column
        // (NaT ignored).
        assert_eq!(
            tsus(
                "t",
                vec![
                    Some(us(2021, 6, 15, 0, 0, 0, 0)),
                    Some(us(2021, 1, 1, 0, 0, 0, 0)),
                    None,
                ],
            ),
            "t\n2021-06-15\n2021-01-01\n\"\"\n",
        );

        // Mixed midnight + non-midnight in one column -> full datetime for all
        // (the rule is per column).
        assert_eq!(
            tsus(
                "t",
                vec![
                    Some(us(2021, 6, 15, 0, 0, 0, 0)),
                    Some(us(2021, 6, 16, 13, 45, 30, 0)),
                ],
            ),
            "t\n2021-06-15 00:00:00\n2021-06-16 13:45:30\n",
        );

        // Microsecond precision -> 6 fractional digits, uniform across column.
        assert_eq!(
            tsus(
                "t",
                vec![
                    Some(us(2021, 6, 15, 13, 45, 30, 123456)),
                    Some(us(2021, 6, 15, 13, 45, 30, 0)),
                ],
            ),
            "t\n2021-06-15 13:45:30.123456\n2021-06-15 13:45:30.000000\n",
        );

        // Finest sub-second is milliseconds -> 3 fractional digits (uniform).
        assert_eq!(
            tsus(
                "t",
                vec![
                    Some(us(2021, 6, 15, 13, 45, 30, 500000)),
                    Some(us(2021, 6, 15, 13, 45, 30, 0)),
                ],
            ),
            "t\n2021-06-15 13:45:30.500\n2021-06-15 13:45:30.000\n",
        );

        // Nanosecond precision -> 9 fractional digits.
        assert_eq!(
            single_col_csv(
                Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
                Arc::new(TimestampNanosecondArray::from(vec![Some(ns(
                    2021, 6, 15, 13, 45, 30, 123456789,
                ))])),
            ),
            "t\n2021-06-15 13:45:30.123456789\n",
        );

        // Date32 / Date64 -> YYYY-MM-DD (NaT -> empty NA).
        assert_eq!(
            single_col_csv(
                Field::new("d", DataType::Date32, true),
                Arc::new(Date32Array::from(vec![
                    Some(d32(2021, 6, 15)),
                    None,
                    Some(d32(2021, 1, 1)),
                ])),
            ),
            "d\n2021-06-15\n\"\"\n2021-01-01\n",
        );
        assert_eq!(
            single_col_csv(
                Field::new("d", DataType::Date64, true),
                Arc::new(Date64Array::from(vec![
                    Some(d64(2021, 6, 15)),
                    Some(d64(2021, 1, 1))
                ])),
            ),
            "d\n2021-06-15\n2021-01-01\n",
        );

        // Time is a pandas object column of datetime.time: per-element
        // isoformat, six digits when microseconds present, none otherwise —
        // NOT the uniform {3,6,9} timestamp width.
        assert_eq!(
            single_col_csv(
                Field::new("t", DataType::Time64(TimeUnit::Microsecond), true),
                Arc::new(Time64MicrosecondArray::from(vec![
                    Some(t64us(13, 45, 30, 500000)),
                    Some(t64us(1, 2, 3, 0)),
                    None,
                ])),
            ),
            "t\n13:45:30.500000\n01:02:03\n\"\"\n",
        );
        assert_eq!(
            single_col_csv(
                Field::new("t", DataType::Time32(TimeUnit::Second), true),
                Arc::new(Time32SecondArray::from(vec![Some(t32s(1, 2, 3))])),
            ),
            "t\n01:02:03\n",
        );
        // Time32[ms] with 500ms -> microseconds field = 500000, six digits.
        assert_eq!(
            single_col_csv(
                Field::new("t", DataType::Time32(TimeUnit::Millisecond), true),
                Arc::new(Time32MillisecondArray::from(vec![Some(t32ms(
                    1, 2, 3, 500
                ))])),
            ),
            "t\n01:02:03.500000\n",
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

    /// A zero-column table (an empty dataset) must serialize to an empty CSV, not
    /// error. The normalize pass rebuilds each batch, and `RecordBatch::try_new`
    /// cannot infer a row count from zero columns; carrying the source batch's
    /// row count is what keeps the rebuild valid.
    #[test]
    fn csv_zero_column_table_serializes_empty() {
        use arrow::record_batch::RecordBatchOptions;
        let schema = Arc::new(Schema::new(Vec::<Field>::new()));
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(0)),
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut w = FileWriter::try_new(&mut ipc, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        let reg = table_registry();
        let ser = reg
            .dispatch(StructureFamily::Table, mime::CSV)
            .expect("text/csv registered");
        let out = ser(&ipc, &serde_json::Value::Null)
            .expect("zero-column table must serialize, not error");
        // The point of the fix is that this no longer errors. Arrow's CSV writer
        // emits `""\n` for a zero-column batch (pandas would print a bare `\n`);
        // that byte-level divergence is a pre-existing arrow-writer quirk, not
        // what finding 8 is about — it only requires the empty export to be a
        // 200 rather than a 500.
        assert_eq!(
            String::from_utf8(out.to_vec()).unwrap(),
            "\"\"\n",
            "empty table CSV is arrow's zero-column form"
        );
    }
}
