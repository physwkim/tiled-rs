//! Core response construction logic.
//!
//! Corresponds to `tiled/server/core.py` — `construct_resource`, `construct_entries_response`.

use std::sync::Arc;

use crate::core::adapters::{
    AnyAdapter, BoxFuture, ContainerAdapter, SearchEntry, TableAdapterRead,
};
use crate::core::dtype::{ArrowTable, BuiltinDType, Endianness, Kind};
use crate::core::links;
use crate::core::queries::AccessBlobFilter;
use crate::core::schemas::{
    ContainerMeta, NodeAttributes, NodeStructure, Resource, Response, SortDirection, SortingItem,
};
use crate::core::structures::StructureFamily;

use crate::server::error::ServerError;

/// Walk the adapter tree to find a node at the given path.
///
/// Takes pre-split segments (already percent-decoded by the extractor) so
/// keys containing literal `/` (sent as `%2F`) reach `get()` intact.
///
/// Returns an **owned** [`AnyAdapter`] (a cheap `Arc` bump): the async
/// `ContainerAdapter::get` hands back owned children, so each hop resolves one
/// key lazily (one `fetch_child` for the SQL catalog) instead of materialising
/// a whole level just to borrow into it. A `get` that fails (DB error) is an
/// `Err`, never silently "not found".
#[tracing::instrument(skip(root))]
pub async fn walk_tree(
    root: &dyn ContainerAdapter,
    segments: &[String],
) -> Result<AnyAdapter, ServerError> {
    if segments.is_empty() {
        return Err(ServerError::NotFound("Use root directly".into()));
    }

    let last = segments.len() - 1;
    // First hop from the borrowed root; every later hop descends into the
    // owned container returned by the previous `get`.
    let mut current = root
        .get(&segments[0])
        .await?
        .ok_or_else(|| ServerError::NotFound(format!("Key not found: {}", segments[0])))?;

    for j in 1..=last {
        let parent = match current {
            AnyAdapter::Container(c) => c,
            // Final hop into a table addresses one of its columns as a
            // synthesized array node. Upstream `TableAdapter.get(column)` returns
            // `ArrayAdapter.from_array(self.read([column])[column].values)`
            // (adapters/table.py:143-146, arrow.py:95-98), so a table is
            // descendable by column name even though it is a leaf, not a
            // container. Only the FINAL segment may name a column — a column is
            // an array leaf, so a path continuing past it cannot resolve.
            AnyAdapter::Table(t) if j == last => {
                return table_column_as_array(&t, &segments[j]).await;
            }
            _ => {
                return Err(ServerError::NotFound(format!(
                    "'{}' is not a container, cannot descend further",
                    segments[j - 1]
                )));
            }
        };
        current = parent
            .get(&segments[j])
            .await?
            .ok_or_else(|| ServerError::NotFound(format!("Key not found: {}", segments[j])))?;
    }

    Ok(current)
}

/// Synthesize an array-adapter view over a single `column` of a `table` node.
///
/// Parity with upstream `TableAdapter.__getitem__` / `.get`
/// (adapters/table.py:137-146, arrow.py:95-98): a table column is addressable as
/// a child *array* node, materialized as
/// `ArrayAdapter.from_array(self.read([column])[column].values)`. The column's
/// dtype comes from the table's Arrow schema and its shape is `[nrows]`; a name
/// absent from the schema's `columns` is a 404 (upstream `get` returns `None`).
///
/// The whole column is read and materialized eagerly here — exactly as upstream
/// `__getitem__` reads `self.read([column])` to build the `ArrayAdapter` — so the
/// data routes (`/array/full`, `/array/block`, `/zarr/…`) slice an in-memory
/// array with no further reads.
async fn table_column_as_array(
    table: &Arc<dyn TableAdapterRead>,
    column: &str,
) -> Result<AnyAdapter, ServerError> {
    // Absent column → 404, before any read (upstream `get` → None). Uses the
    // same "Key not found" message as a missing container child so both walk
    // failures surface identically.
    if !table.structure().columns.iter().any(|c| c == column) {
        return Err(ServerError::NotFound(format!("Key not found: {column}")));
    }
    // Read only the requested column (a one-column projection), then materialize
    // it as numpy little-endian bytes + dtype — upstream's
    // `self.read([column])[column].values`.
    let fields = [column.to_string()];
    let projected = table.read(Some(&fields)).await.map_err(ServerError::from)?;
    let (data, dtype, nrows) = arrow_column_to_numpy(&projected, column)?;
    let adapter = crate::adapters::ArrayAdapter::from_array(
        data,
        dtype,
        vec![nrows],
        // A single-partition column reads as one chunk covering every row
        // (upstream's numpy-backed `ArrayAdapter.from_array` chunks a
        // sub-128-MiB 1-D column as a single chunk).
        vec![vec![nrows]],
        // A synthesized column view carries no user metadata / specs, matching
        // upstream `ArrayAdapter.from_array(array)` (neither is passed there).
        serde_json::json!({}),
        vec![],
    );
    Ok(AnyAdapter::Array(Arc::new(adapter)))
}

/// Convert a single-column [`ArrowTable`] (already projected to one column) into
/// a numpy little-endian byte buffer, its [`BuiltinDType`], and the row count.
///
/// For fixed-width types (numeric, boolean, temporal) the dtype comes from the
/// Arrow schema field, so it is known even for a zero-row column; the bytes are
/// the column concatenated across all partitions/batches in C order.
///
/// **Schema-only invariant is relaxed for strings.** A string column becomes
/// numpy fixed-width unicode `<U{n}`, and `n` is the longest value's char count
/// over the *concatenated* column — data the schema alone does not carry. So a
/// string column is concatenated first and its dtype derived from the data (see
/// [`arrow_string_column_to_numpy`]), unlike every other type here whose dtype
/// is fixed by the schema field.
fn arrow_column_to_numpy(
    table: &ArrowTable,
    column: &str,
) -> Result<(bytes::Bytes, BuiltinDType, usize), ServerError> {
    use arrow::array::Array;
    use arrow::datatypes::DataType;

    let field = table.schema.field(0);
    let arrays: Vec<&dyn Array> = table.batches.iter().map(|b| b.column(0).as_ref()).collect();
    let nrows: usize = arrays.iter().map(|a| a.len()).sum();

    // Strings size their `<U{n}` dtype from the data, so branch before the
    // schema-based dtype derivation and concat inside the string path.
    if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
        return arrow_string_column_to_numpy(&arrays, nrows, column);
    }

    let dtype = arrow_datatype_to_builtin(field.data_type(), column)?;
    if nrows == 0 {
        return Ok((bytes::Bytes::new(), dtype, 0));
    }
    // One contiguous column across partitions (upstream concatenates partitions
    // before exposing the column via `read`).
    let col = arrow::compute::concat(&arrays)
        .map_err(|e| ServerError::Internal(format!("concat column '{column}': {e}")))?;
    let bytes = arrow_array_to_le_bytes(col.as_ref(), column)?;
    Ok((bytes::Bytes::from(bytes), dtype, nrows))
}

/// Materialize a string column as numpy fixed-width unicode `<U{n}` bytes.
///
/// Parity with upstream `ArrayAdapter.from_array` on a pandas object/string
/// column: `numpy.array([str(x) for x in array])` (adapters/array.py:73-78),
/// which yields a `<U{n}` array whose width `n` is the longest value's char
/// count. numpy stores unicode as UCS4/UTF-32, so each element occupies `4 * n`
/// bytes; each code point is emitted little-endian and rows shorter than `n` are
/// right-padded with U+0000. A null renders as the literal string `"None"` —
/// Python's `str(None)` (adapters/array.py:78) — not a masked or empty slot. An
/// empty column has width 0 (`<U0`, an empty buffer).
fn arrow_string_column_to_numpy(
    arrays: &[&dyn arrow::array::Array],
    nrows: usize,
    column: &str,
) -> Result<(bytes::Bytes, BuiltinDType, usize), ServerError> {
    use arrow::array::{Array, LargeStringArray, StringArray};
    use arrow::datatypes::DataType;

    let downcast_err = |ty: &str| -> ServerError {
        ServerError::Internal(format!("column '{column}': downcast to {ty} failed"))
    };

    // One `String` per row, nulls rendered as "None" (upstream `str(None)`).
    let mut values: Vec<String> = Vec::with_capacity(nrows);
    for arr in arrays {
        match arr.data_type() {
            DataType::Utf8 => {
                let a = arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| downcast_err("StringArray"))?;
                for i in 0..a.len() {
                    values.push(if a.is_null(i) {
                        "None".to_string()
                    } else {
                        a.value(i).to_string()
                    });
                }
            }
            DataType::LargeUtf8 => {
                let a = arr
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| downcast_err("LargeStringArray"))?;
                for i in 0..a.len() {
                    values.push(if a.is_null(i) {
                        "None".to_string()
                    } else {
                        a.value(i).to_string()
                    });
                }
            }
            other => {
                return Err(ServerError::Internal(format!(
                    "column '{column}': string path reached non-string type {other:?}"
                )));
            }
        }
    }

    // Width = longest value's char count; numpy stores 4 bytes/char (UCS4).
    let max_chars = values.iter().map(|s| s.chars().count()).max().unwrap_or(0);
    let itemsize = max_chars * 4;
    let mut out = Vec::with_capacity(nrows * itemsize);
    for s in &values {
        let mut chars = 0usize;
        for ch in s.chars() {
            out.extend_from_slice(&(ch as u32).to_le_bytes());
            chars += 1;
        }
        // Right-pad the fixed-width cell with U+0000.
        for _ in chars..max_chars {
            out.extend_from_slice(&0u32.to_le_bytes());
        }
    }
    let dtype = BuiltinDType::new(Endianness::Little, Kind::Unicode, itemsize);
    Ok((bytes::Bytes::from(out), dtype, nrows))
}

/// Map an Arrow numeric/boolean/temporal [`arrow::datatypes::DataType`] to the
/// numpy [`BuiltinDType`] a column-array view exposes. One-byte types use
/// numpy's "not applicable" byte-order marker (`|i1`, `|u1`, `|b1`); multi-byte
/// numerics are little-endian (the byte order [`arrow_array_to_le_bytes`]
/// emits).
///
/// Temporal columns become numpy `datetime64` (`<M8[unit]`), an 8-byte little-
/// endian int64 tick count whose unit is carried in `dt_units`: `Timestamp`
/// keeps its Arrow unit (`[s]`/`[ms]`/`[us]`/`[ns]`), `Date32` is `[D]`, and
/// `Date64` is `[ms]`. A `Timestamp`'s timezone is dropped — numpy `datetime64`
/// is tz-naive, so the ticks are the raw UTC ticks upstream also serves after
/// `numpy.array(series.values)` on a tz-aware column (the tz is not surfaced in
/// metadata). A non-numeric, non-temporal column (nested, decimal, …) is
/// rejected — the array routes cannot serve it. Strings are handled earlier in
/// [`arrow_column_to_numpy`] and never reach here.
fn arrow_datatype_to_builtin(
    dt: &arrow::datatypes::DataType,
    column: &str,
) -> Result<BuiltinDType, ServerError> {
    use arrow::datatypes::{DataType, TimeUnit};

    // datetime64 unit token for a temporal `<M8[unit]` dtype.
    let time_unit = |u: &TimeUnit| match u {
        TimeUnit::Second => "[s]",
        TimeUnit::Millisecond => "[ms]",
        TimeUnit::Microsecond => "[us]",
        TimeUnit::Nanosecond => "[ns]",
    };
    let datetime64 = |units: &str| BuiltinDType {
        endianness: Endianness::Little,
        kind: Kind::Datetime,
        itemsize: 8,
        dt_units: Some(units.to_string()),
    };

    let (kind, size, endian) = match dt {
        DataType::Int8 => (Kind::Integer, 1, Endianness::NotApplicable),
        DataType::Int16 => (Kind::Integer, 2, Endianness::Little),
        DataType::Int32 => (Kind::Integer, 4, Endianness::Little),
        DataType::Int64 => (Kind::Integer, 8, Endianness::Little),
        DataType::UInt8 => (Kind::UnsignedInteger, 1, Endianness::NotApplicable),
        DataType::UInt16 => (Kind::UnsignedInteger, 2, Endianness::Little),
        DataType::UInt32 => (Kind::UnsignedInteger, 4, Endianness::Little),
        DataType::UInt64 => (Kind::UnsignedInteger, 8, Endianness::Little),
        DataType::Float32 => (Kind::Float, 4, Endianness::Little),
        DataType::Float64 => (Kind::Float, 8, Endianness::Little),
        DataType::Boolean => (Kind::Boolean, 1, Endianness::NotApplicable),
        // datetime64: 8-byte int64 ticks, unit carried in `dt_units`.
        DataType::Timestamp(unit, _tz) => return Ok(datetime64(time_unit(unit))),
        DataType::Date32 => return Ok(datetime64("[D]")),
        DataType::Date64 => return Ok(datetime64("[ms]")),
        other => {
            return Err(ServerError::WrongType(format!(
                "table column '{column}' has type {other:?}, which cannot be served as an array"
            )));
        }
    };
    Ok(BuiltinDType::new(endian, kind, size))
}

/// Serialize an Arrow numeric/boolean array to numpy C-order little-endian
/// bytes. Mirrors `hdf5_common::write_table_column`'s per-type handling: integer
/// values are copied from the underlying values buffer; float nulls become NaN
/// (matching the JSON and HDF5 serializers); boolean stores as u8 (0/1),
/// null → 0.
fn arrow_array_to_le_bytes(
    array: &dyn arrow::array::Array,
    column: &str,
) -> Result<Vec<u8>, ServerError> {
    use arrow::array::{
        Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array,
        Int16Array, Int32Array, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    };
    use arrow::datatypes::{DataType, TimeUnit};

    let n = array.len();
    let downcast_err = |ty: &str| -> ServerError {
        ServerError::Internal(format!("column '{column}': downcast to {ty} failed"))
    };

    // Integer: copy the underlying values buffer verbatim (null slots carry
    // their buffer value, as in hdf5_common — pandas promotes genuinely
    // null-bearing integer columns to float before they reach here).
    macro_rules! ints {
        ($arr:ty, $native:ty, $ty_name:literal) => {{
            let a = array
                .as_any()
                .downcast_ref::<$arr>()
                .ok_or_else(|| downcast_err($ty_name))?;
            let mut out = Vec::with_capacity(n * std::mem::size_of::<$native>());
            for &v in a.values().iter() {
                out.extend_from_slice(&v.to_le_bytes());
            }
            out
        }};
    }
    // Float: map each slot, turning Arrow nulls into NaN.
    macro_rules! floats {
        ($arr:ty, $native:ty, $ty_name:literal) => {{
            let a = array
                .as_any()
                .downcast_ref::<$arr>()
                .ok_or_else(|| downcast_err($ty_name))?;
            let mut out = Vec::with_capacity(n * std::mem::size_of::<$native>());
            for i in 0..n {
                let v = if a.is_null(i) {
                    <$native>::NAN
                } else {
                    a.value(i)
                };
                out.extend_from_slice(&v.to_le_bytes());
            }
            out
        }};
    }
    // datetime64: 8-byte little-endian int64 ticks, Arrow null → `i64::MIN`
    // (numpy `NaT`, the sentinel `datetime64` uses for a missing value). `$widen`
    // lifts a narrower native tick (`Date32`'s i32 days) to i64.
    macro_rules! datetimes {
        ($arr:ty, $ty_name:literal, $widen:expr) => {{
            let a = array
                .as_any()
                .downcast_ref::<$arr>()
                .ok_or_else(|| downcast_err($ty_name))?;
            let mut out = Vec::with_capacity(n * 8);
            for i in 0..n {
                let v: i64 = if a.is_null(i) {
                    i64::MIN
                } else {
                    $widen(a.value(i))
                };
                out.extend_from_slice(&v.to_le_bytes());
            }
            out
        }};
    }

    let bytes = match array.data_type() {
        DataType::Int8 => ints!(Int8Array, i8, "Int8Array"),
        DataType::Int16 => ints!(Int16Array, i16, "Int16Array"),
        DataType::Int32 => ints!(Int32Array, i32, "Int32Array"),
        DataType::Int64 => ints!(Int64Array, i64, "Int64Array"),
        DataType::UInt8 => ints!(UInt8Array, u8, "UInt8Array"),
        DataType::UInt16 => ints!(UInt16Array, u16, "UInt16Array"),
        DataType::UInt32 => ints!(UInt32Array, u32, "UInt32Array"),
        DataType::UInt64 => ints!(UInt64Array, u64, "UInt64Array"),
        DataType::Float32 => floats!(Float32Array, f32, "Float32Array"),
        DataType::Float64 => floats!(Float64Array, f64, "Float64Array"),
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| downcast_err("BooleanArray"))?;
            // Booleans are bit-packed in Arrow, so they cannot be copied like a
            // values buffer; expand to one u8 per element (null → 0).
            (0..n)
                .map(|i| u8::from(!a.is_null(i) && a.value(i)))
                .collect()
        }
        // datetime64 ticks (int64). Timestamp/Date64 are already i64; Date32 is
        // i32 days widened to i64. The dtype's unit (from
        // `arrow_datatype_to_builtin`) tells the client how to read these ticks.
        DataType::Timestamp(unit, _tz) => match unit {
            TimeUnit::Second => datetimes!(TimestampSecondArray, "TimestampSecondArray", |v| v),
            TimeUnit::Millisecond => {
                datetimes!(
                    TimestampMillisecondArray,
                    "TimestampMillisecondArray",
                    |v| v
                )
            }
            TimeUnit::Microsecond => {
                datetimes!(
                    TimestampMicrosecondArray,
                    "TimestampMicrosecondArray",
                    |v| v
                )
            }
            TimeUnit::Nanosecond => {
                datetimes!(TimestampNanosecondArray, "TimestampNanosecondArray", |v| v)
            }
        },
        DataType::Date32 => datetimes!(Date32Array, "Date32Array", |v: i32| v as i64),
        DataType::Date64 => datetimes!(Date64Array, "Date64Array", |v| v),
        other => {
            return Err(ServerError::WrongType(format!(
                "table column '{column}' has type {other:?}, which cannot be served as an array"
            )));
        }
    };
    Ok(bytes)
}

/// Compute ancestors list from a segment list.
///
/// Matches Python tiled's wire format: `["a", "b", "c"]` → `["a", "b"]`,
/// `["a"]` → `[]`, `[]` → `[]`.
pub fn ancestors_from_segments(segments: &[String]) -> Vec<String> {
    if segments.len() <= 1 {
        return vec![];
    }
    segments[..segments.len() - 1].to_vec()
}

/// Backwards-compat helper: split a slash-joined path and compute ancestors.
/// New code should prefer [`ancestors_from_segments`].
pub fn ancestors_from_path(path: &str) -> Vec<String> {
    let segments: Vec<String> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    ancestors_from_segments(&segments)
}

/// Default container sorting (ascending by insertion order).
#[inline]
fn default_sorting() -> Vec<SortingItem> {
    vec![SortingItem {
        key: "_".into(),
        direction: crate::core::schemas::SortDirection::Ascending,
    }]
}

/// Upstream `INLINED_CONTENTS_LIMIT` (`tiled/server/core.py:56`): a container
/// with more than this many children is never inlined — its `structure.contents`
/// stays `None` even when inlining is otherwise enabled — to bound response size.
const INLINED_CONTENTS_LIMIT: usize = 500;

/// Per-node response shaping applied to *every* node the response construction
/// builds — the addressed node AND every inlined child (Wave-35 Finding 2).
///
/// Upstream passes `fields`, `select_metadata` and `omit_links` down the
/// `construct_resource` recursion and applies them to each node it constructs
/// (`tiled/server/core.py:485-583`). The port builds a full [`Resource`] and
/// shapes it afterwards; [`shape_resource`] is the single implementation of that
/// shaping, so the addressed node (shaped by the handler) and every inlined
/// child (shaped by [`build_container_structure`]) go through the exact same
/// rule.
///
/// `include_data_sources` is deliberately absent: the inline walk resolves
/// children through `ContainerAdapter::get` → [`AnyAdapter`], which carries no
/// `data_sources` accessor (the catalog populates that only in the top-level
/// `search_page` batch, keyed by node id). For in-memory trees this matches
/// upstream (in-memory adapters have no `data_sources`, so upstream's
/// `hasattr(entry, "data_sources")` gate is false); the catalog inline path is
/// the only residual divergence and closing it needs a trait-level child
/// data-sources accessor — see the Wave-35 Finding 2 report.
#[derive(Clone, Copy, Default)]
pub struct ShapeOptions<'a> {
    /// `?select_metadata=` JMESPath expression, applied within `metadata`.
    pub select_metadata: Option<&'a str>,
    /// `?fields=` section projection; `None` means "full entry" (no pruning).
    pub fields: Option<&'a [String]>,
    /// `?omit_links=true` drops the per-node `links`.
    pub omit_links: bool,
}

/// Apply a JMESPath expression to node metadata, mirroring Python
/// `core.py:486-489`.
///
/// When `select_metadata` is `Some(expr)`, compiles and evaluates the expression
/// against the metadata JSON and returns `{"selected": <result>}`. On compile or
/// evaluation error → `BadRequest` (HTTP 400), matching Python's `JMESPathError
/// → HTTP_400_BAD_REQUEST` (`router.py:395-399 / 503-507`). When `select_metadata`
/// is `None`, returns `metadata` unchanged.
pub(crate) fn apply_select_metadata(
    select_metadata: Option<&str>,
    metadata: Option<serde_json::Value>,
) -> Result<Option<serde_json::Value>, ServerError> {
    let expr_str = match select_metadata {
        None => return Ok(metadata),
        Some(e) => e,
    };
    let expr = jmespath::compile(expr_str).map_err(|e| {
        ServerError::BadRequest(format!(
            "Malformed 'select_metadata' parameter raised JMESPathError: {e}"
        ))
    })?;
    let meta = metadata.unwrap_or(serde_json::Value::Null);
    // Round-trip through JSON string: serde_json::Value → &str → jmespath::Variable.
    // serde_json::to_string on a Value never fails; from_json on its output
    // also never fails, so both conversions are infallible here.
    let json_str =
        serde_json::to_string(&meta).expect("serde_json::Value always serializes to JSON");
    let var = jmespath::Variable::from_json(&json_str)
        .expect("JSON produced by serde_json::to_string always parses");
    let result = expr.search(var).map_err(|e| {
        ServerError::BadRequest(format!(
            "Malformed 'select_metadata' parameter raised JMESPathError: {e}"
        ))
    })?;
    // Variable: Display renders JSON; parse back to serde_json::Value.
    let selected: serde_json::Value =
        serde_json::from_str(&result.to_string()).unwrap_or(serde_json::Value::Null);
    Ok(Some(serde_json::json!({"selected": selected})))
}

/// Apply the `?fields=` projection to one entry's attributes, mirroring Python
/// `EntryFields` (`schemas.py`) as consumed by `construct_resource`
/// (`core.py:485-577`) and the id-only `fields=""` shape (`core.py:248`).
///
/// `requested` is the set of `fields` query values the client sent. Each named
/// attribute section is retained only when its `EntryFields` value is present;
/// every other section is set to `None` and dropped by `skip_serializing_if`.
/// `ancestors` is always kept (an id-only `fields=""` resource still carries it,
/// `core.py:248`). Recognized names: `metadata`, `structure_family`, `structure`,
/// `specs`, `sorting`, `access_blob`. `count` and the empty value (`none`)
/// request no attribute section; unknown names are ignored.
///
/// `data_sources` is deliberately NOT pruned here: upstream sets it from the
/// `include_data_sources` flag alone (`core.py:483`), independent of `fields`, so
/// a `fields=metadata&include_data_sources=true` request keeps its data sources.
/// It is `None` unless that flag was set, so leaving it untouched is a no-op for
/// every request that did not ask for it.
///
/// The caller MUST invoke this only when the client actually sent `fields` — an
/// absent `fields` means "full entry" (the FastAPI default selects every
/// `EntryFields`), which is the unpruned state and must not be pruned to nothing.
pub(crate) fn prune_entry_fields(attrs: &mut NodeAttributes, requested: &[String]) {
    let want = |f: &str| requested.iter().any(|r| r == f);
    if !want("metadata") {
        attrs.metadata = None;
    }
    if !want("structure_family") {
        attrs.structure_family = None;
    }
    if !want("structure") {
        attrs.structure = None;
    }
    if !want("specs") {
        attrs.specs = None;
    }
    if !want("sorting") {
        attrs.sorting = None;
    }
    if !want("access_blob") {
        attrs.access_blob = None;
    }
}

/// Apply per-node response shaping to one built [`Resource`], in upstream order
/// (`core.py:485-583`):
///
/// 1. `select_metadata` within `metadata` — but only when the `fields`
///    projection keeps `metadata` (`core.py:485-489`): if the projection
///    excludes `metadata`, upstream never compiles the expression, so a
///    malformed one cannot 400 a request that was not asking for metadata.
/// 2. the `fields` section projection ([`prune_entry_fields`]), applied last so
///    it strips any section `select_metadata` populated but the client did not
///    request.
/// 3. `omit_links` — drop the per-node `links`.
///
/// This is the single owner of the shaping rule: the addressed node is shaped by
/// each handler and every inlined child by [`build_container_structure`], so the
/// whole recursion is shaped uniformly (Wave-35 Finding 2).
pub(crate) fn shape_resource(
    resource: &mut Resource,
    opts: ShapeOptions<'_>,
) -> Result<(), ServerError> {
    let metadata_in_fields = opts
        .fields
        .is_none_or(|f| f.iter().any(|r| r == "metadata"));
    if metadata_in_fields && opts.select_metadata.is_some() {
        resource.attributes.metadata =
            apply_select_metadata(opts.select_metadata, resource.attributes.metadata.take())?;
    }
    if let Some(requested) = opts.fields {
        prune_entry_fields(&mut resource.attributes, requested);
    }
    if opts.omit_links {
        resource.links = crate::core::schemas::NodeLinks::default();
    }
    Ok(())
}

/// Construct a Resource for a given adapter.
///
/// Returns a boxed future because the container arm recurses (a container that
/// [asks to inline](ContainerAdapter::inlined_contents_enabled) builds each
/// child's full Resource one level deeper). `max_depth`/`depth` drive the
/// upstream inline gate (`tiled/server/core.py:513-516`): `depth` is the walk
/// level (0 at the addressed node) and `max_depth` the client's `?max_depth=`
/// bound (`None` ⇒ inline down to `DEPTH_LIMIT`). A leaf ignores both and simply
/// carries its own structure.
///
/// `access_filter` is the caller's list filter (the same `AccessBlobFilter`
/// `/search` injects at the top level). It is **required** — every caller must
/// pass it — so the inline walk can never enumerate children the caller may not
/// see (Wave-35 Finding 1; see [`build_container_structure`]). `None` means "no
/// access policy is in force" (inline every child); a leaf never enumerates
/// children, so it ignores the argument.
///
/// `shape` carries the per-node response shaping (`select_metadata`, `fields`,
/// `omit_links`). It is **not** applied to the node this call returns — the
/// caller shapes the node it places into a response (a handler shapes the
/// addressed node; [`build_container_structure`] shapes each inlined child) —
/// but it is threaded into the inline walk so every inlined child is shaped
/// (Wave-35 Finding 2).
#[allow(clippy::too_many_arguments)]
pub fn construct_resource<'a>(
    adapter: &'a AnyAdapter,
    id: &'a str,
    path: &'a str,
    base_url: &'a str,
    max_depth: Option<usize>,
    depth: usize,
    access_filter: Option<&'a AccessBlobFilter>,
    shape: ShapeOptions<'a>,
) -> BoxFuture<'a, Result<Resource, ServerError>> {
    Box::pin(async move {
        let family = adapter.structure_family();
        let node_links = links::links_for_node(family, base_url, path);

        let sorting = match adapter {
            AnyAdapter::Container(_) => Some(default_sorting()),
            _ => None,
        };

        // A container's structure carries the child `count` and, when the inline
        // gate passes, the children's full Resources under `contents`; a leaf
        // carries its own array/table structure unchanged.
        let structure = match adapter {
            AnyAdapter::Container(c) => Some(
                build_container_structure(
                    c.as_ref(),
                    path,
                    base_url,
                    max_depth,
                    depth,
                    access_filter,
                    shape,
                )
                .await?,
            ),
            _ => adapter.structure_json().await?,
        };

        Ok(Resource {
            id: id.to_string(),
            attributes: NodeAttributes {
                ancestors: ancestors_from_path(path),
                structure_family: Some(family),
                specs: Some(adapter.specs().to_vec()),
                metadata: Some(adapter.metadata().clone()),
                structure,
                access_blob: None,
                sorting,
                data_sources: None,
            },
            links: node_links,
        })
    })
}

/// Build a container's wire `structure` (`{"contents": …, "count": N}`),
/// inlining its children's full Resources when the upstream gate passes
/// (`tiled/server/core.py:513-556`).
///
/// The single inlining owner. Three callers route through it so the gate is
/// enforced in exactly one place: in-memory `/metadata` ([`construct_resource`]),
/// `/search` entries ([`construct_entries_response`]), and the catalog top node
/// (`catalog_metadata_resource`, `router.rs`). Each caller pre-filters cheaply on
/// the `"xarray_dataset"` spec discriminator to avoid resolving a plain
/// container's children, then defers to the authoritative gate here.
///
/// Mirrors upstream exactly:
///
/// - Gate: `(max_depth is None || depth < max_depth) && inlined_contents_enabled(depth)
///   && depth <= DEPTH_LIMIT`. When it fails, `contents` is `null` (an explicit
///   JSON `null`, matching the prior non-inlined shape and upstream's
///   `NodeStructure(contents=None)` dump) and `count` is the visible child count.
/// - Size cap: a container whose *visible* count already exceeds
///   [`INLINED_CONTENTS_LIMIT`] is not inlined (`contents = null`). While
///   walking, if the true visible count crosses the cap (the estimate was low or
///   the container grew), inlining is abandoned (`contents = null`) and `count` is
///   recomputed. Otherwise `count` becomes the exact walked (visible) count.
/// - A key that `keys()` listed but `get()` cannot resolve — a broken link
///   (upstream `BrokenLink`) or a concurrent delete — is kept as an explicit
///   `null` value under its key (upstream `contents[key] = None`); a genuine
///   lookup error (DB/IO) propagates.
///
/// # Access filter (Wave-35 Finding 1)
///
/// This is a **caller-facing child enumeration**, and the project invariant
/// (zarr fix 82a7041) is: every such enumeration MUST route through the caller's
/// access filter — no inline path may enumerate children the caller's
/// `list_filter` would hide. `build_container_structure` is the single owner of
/// that gate for the whole inline family (`/search` entry inlining, in-memory
/// `/metadata` inlining, and the held container/full top-node branch).
///
/// When `access_filter` is `Some`, the permitted child set is computed **first**
/// from the same filtered listing `/search` uses —
/// `container.search(&[AccessBlobFilter])` — and drives everything downstream: the
/// inline gate, the [`INLINED_CONTENTS_LIMIT`] cap, the walk, AND `count`. A child
/// absent from the permitted set is skipped (never resolved, absent from
/// `contents`, uncounted), exactly as if it were not listed. When `access_filter`
/// is `None` (no policy configured) every child is visible, preserving the prior
/// behaviour for policy-free deployments and in-memory trees.
///
/// `count` is **principal-scoped**: it is the number of children the caller may
/// see, so it equals the number of `contents` entries when inlined and never
/// exceeds it. This matches upstream, which computes `count` via `len_or_approx`
/// over the `filter_for_access`-wrapped view (`core.py:509`) and recounts over the
/// filtered keys during the walk (`core.py:527-529`); the `INLINED_CONTENTS_LIMIT`
/// cap likewise applies to the filtered view (`core.py:520,530`). Because the
/// visible count drives the cap, a mostly-hidden large container inlines its
/// (few) visible children instead of being suppressed by its full cardinality.
/// (With a filter the permitted-key count is exact rather than approximate — the
/// `len_or_approx` approximation divergence for filtered callers is an accepted
/// residual, since the filtered set is materialized to enumerate it anyway.)
///
/// # Per-node shaping (Wave-35 Finding 2)
///
/// This is the single owner of the inline walk, so it is also where each inlined
/// child is shaped: after [`construct_resource`] builds a child, [`shape_resource`]
/// applies `shape` (`select_metadata` / `fields` / `omit_links`) to it, matching
/// upstream's per-node application down the recursion (`core.py:485-583`). The
/// addressed top-level node is shaped by its handler, not here.
pub(crate) async fn build_container_structure(
    container: &dyn ContainerAdapter,
    path: &str,
    base_url: &str,
    max_depth: Option<usize>,
    depth: usize,
    access_filter: Option<&AccessBlobFilter>,
    shape: ShapeOptions<'_>,
) -> Result<serde_json::Value, ServerError> {
    // The caller-visible child set, computed FIRST so it drives the count, the
    // inline gate, the size cap, and the walk uniformly. With an access filter in
    // force, only keys the filter admits are visible — resolved via the SAME
    // filtered listing `/search` injects (`AccessBlobFilter`), never raw `keys()`
    // (zarr-fix invariant). `None` (no policy) means every child is visible.
    let permitted: Option<std::collections::HashSet<String>> = match access_filter {
        Some(f) => Some(
            container
                .search(&[crate::core::queries::Query::AccessBlobFilter(f.clone())])
                .await?
                .into_iter()
                .collect(),
        ),
        None => None,
    };

    // Principal-scoped `count` (upstream `len_or_approx` over the filtered view,
    // core.py:509): the exact visible-key count with a filter, else the
    // container's own (possibly approximate) count.
    let mut count = match permitted {
        Some(ref p) => p.len(),
        None => container.len().await?,
    };

    // Upstream inline gate (core.py:513-516). `max_depth is None` inlines down
    // to the `depth <= DEPTH_LIMIT` bound; a set `max_depth` stops one level
    // shallower via `depth < max_depth`.
    let gate = max_depth.is_none_or(|m| depth < m)
        && container.inlined_contents_enabled(depth)
        && depth <= links::DEPTH_LIMIT;

    let contents: Option<serde_json::Map<String, serde_json::Value>> = if gate {
        if count > INLINED_CONTENTS_LIMIT {
            // Visible count already too large: do not inline.
            None
        } else {
            let keys = container.keys().await?;
            let mut map = serde_json::Map::new();
            let mut walked = 0usize;
            let mut too_large = false;
            for key in keys {
                // Access gate FIRST: a child the caller's filter hides is absent
                // from `contents` (as if unlisted), never resolved, and — unlike
                // the prior port behaviour — does NOT count toward the visible
                // walk, the cap, or `count`.
                if let Some(ref permitted) = permitted
                    && !permitted.contains(&key)
                {
                    continue;
                }
                walked += 1;
                if walked > INLINED_CONTENTS_LIMIT {
                    // The estimate was low or the container grew while walking.
                    // (Unreachable with a filter: `walked` is bounded by the exact
                    // visible count, already confirmed `<= INLINED_CONTENTS_LIMIT`.)
                    too_large = true;
                    break;
                }
                let child_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}/{key}")
                };
                match container.get(&key).await? {
                    Some(child) => {
                        let mut child_resource = construct_resource(
                            &child,
                            &key,
                            &child_path,
                            base_url,
                            max_depth,
                            depth + 1,
                            access_filter,
                            shape,
                        )
                        .await?;
                        // Shape this inlined child exactly as a handler shapes
                        // the addressed node — upstream applies `fields` /
                        // `select_metadata` / `omit_links` per node down the
                        // recursion (`core.py:485-583`). Its own children were
                        // already shaped one level deeper by the recursive call.
                        shape_resource(&mut child_resource, shape)?;
                        map.insert(
                            key,
                            serde_json::to_value(child_resource)
                                .expect("Resource is always serializable"),
                        );
                    }
                    // Broken link / concurrent delete: keep the key with a null
                    // value (upstream `contents[key] = None`).
                    None => {
                        map.insert(key, serde_json::Value::Null);
                    }
                }
            }
            if too_large {
                // Re-query the visible count. Only reachable without a filter
                // (with one, the visible count is exact and `<= the cap`), so this
                // is the container's own count; the `permitted` arm is kept for
                // correctness-by-construction.
                count = match permitted {
                    Some(ref p) => p.len(),
                    None => container.len().await?,
                };
                None
            } else {
                count = walked;
                Some(map)
            }
        }
    } else {
        None
    };

    Ok(serde_json::json!({
        "contents": contents,
        "count": count,
    }))
}

/// Construct a Resource for the root container.
pub async fn construct_root_resource(
    root: &dyn ContainerAdapter,
    base_url: &str,
) -> Result<Resource, ServerError> {
    let node_links = links::links_for_node(root.structure_family(), base_url, "");
    let ns = NodeStructure {
        contents: None,
        count: root.len().await?,
    };

    Ok(Resource {
        id: String::new(),
        attributes: NodeAttributes {
            ancestors: vec![],
            structure_family: Some(root.structure_family()),
            specs: Some(root.specs().to_vec()),
            metadata: Some(root.metadata().clone()),
            structure: Some(
                serde_json::to_value(&ns).expect("NodeStructure is always serializable"),
            ),
            access_blob: None,
            sorting: Some(default_sorting()),
            data_sources: None,
        },
        links: node_links,
    })
}

/// Construct a paginated entries response for a container.
///
/// The single listing path for both catalog (SQL pushdown) and in-memory
/// trees: the adapter's [`search_page`](ContainerAdapter::search_page) applies
/// the filters and sort and returns the page of rows plus the **total** match
/// count and the next-page keyset cursor. A `cursor` of `Some(_)` requests the
/// keyset page after it; `None` requests the `[offset, offset+limit)` window.
/// The returned `next_cursor` (set by the SQL catalog under a default sort)
/// drives a `page[cursor]` next link, falling back to `page[offset]` when
/// absent — matching Python's cursor pagination. An unsupported query variant
/// surfaces as `ServerError::UnsupportedQuery` (HTTP 400), matching Python
/// tiled's `UnsupportedQueryType`.
///
/// `max_depth` mirrors the metadata route: each entry is built at walk depth 0,
/// so an entry that is an inline-enabled container gets its children inlined
/// into `structure.contents` when `(max_depth is None || 0 < max_depth)` — i.e.
/// unless `?max_depth=0` (upstream builds each entry via `construct_resource`
/// with `depth=0`, core.py:290).
///
/// `access_filter` is the caller's list filter — the SAME `AccessBlobFilter` the
/// caller injects into `queries` for the top-level `search_page`. It is threaded
/// into [`build_container_structure`] so an inlined entry's children are
/// enumerated through the filter too, never raw `keys()` (Wave-35 Finding 1).
///
/// `shape` carries the per-node response shaping. It is threaded into the inline
/// walk so an inlined entry's children are shaped too; the top-level entries are
/// shaped by the search handler after this returns (Wave-35 Finding 2).
#[allow(clippy::too_many_arguments)]
pub async fn construct_entries_response(
    container: &dyn ContainerAdapter,
    path: &str,
    base_url: &str,
    cursor: Option<i64>,
    offset: usize,
    limit: usize,
    queries: &[crate::core::queries::Query],
    sorting: &[(String, SortDirection)],
    exact_count_limit: u64,
    include_data_sources: bool,
    max_depth: Option<usize>,
    access_filter: Option<&AccessBlobFilter>,
    shape: ShapeOptions<'_>,
) -> Result<Response<Vec<Resource>>, ServerError> {
    let page = container
        .search_page(
            queries,
            sorting,
            cursor,
            offset,
            limit,
            include_data_sources,
        )
        .await?;
    let (entries, next_cursor) = (page.entries, page.next_cursor);
    // Cap the reported total at `exact_count_limit`. When the true count
    // exceeds this value the client receives the limit as a lower-bound
    // estimate, mirroring Python `Settings.exact_count_limit` (settings.py).
    let total = page.total.min(exact_count_limit as usize);
    let path_trimmed = path.trim_matches('/');

    let mut resources: Vec<Resource> = Vec::with_capacity(entries.len());
    for entry in entries {
        let child_path = if path_trimmed.is_empty() {
            entry.key.clone()
        } else {
            format!("{path_trimmed}/{}", entry.key)
        };
        // Inline-eligibility pre-filter, read straight from the row so a listing
        // full of plain containers is not resolved needlessly: a container entry
        // advertising the "xarray_dataset" spec — the same discriminator
        // `ContainerAdapter::inlined_contents_enabled` keys on — is a candidate to
        // inline at depth 0 when `?max_depth=` is absent or `>= 1` (Some(0) means
        // no inlining, mirroring `0 < max_depth` in the gate).
        let inline_candidate = matches!(entry.structure_family, StructureFamily::Container)
            && max_depth.is_none_or(|m| 0 < m)
            && entry.specs.iter().any(|s| s.name == "xarray_dataset");
        let key = entry.key.clone();
        let mut resource = resource_from_entry(entry, &child_path, base_url);
        if inline_candidate {
            // Resolve the child and let `build_container_structure` apply the
            // authoritative gate; it returns `{"contents": null, …}` if the
            // resolved node opts out or exceeds the size cap.
            if let Some(child) = container.get(&key).await?
                && let Some(child_container) = child.as_container()
            {
                resource.attributes.structure = Some(
                    build_container_structure(
                        child_container,
                        &child_path,
                        base_url,
                        max_depth,
                        0,
                        access_filter,
                        shape,
                    )
                    .await?,
                );
            }
        }
        resources.push(resource);
    }

    let pagination = links::pagination_links(
        base_url,
        "search",
        path,
        cursor,
        offset,
        limit,
        next_cursor,
        total,
    );

    Ok(Response {
        data: Some(resources),
        error: None,
        links: Some(
            serde_json::to_value(&pagination).expect("PaginationLinks is always serializable"),
        ),
        meta: Some(
            serde_json::to_value(&ContainerMeta { count: total })
                .expect("ContainerMeta is always serializable"),
        ),
    })
}

/// Build one listing `Resource` from a neutral [`SearchEntry`] row. The
/// `ancestors`, per-child `sorting` and `links` are derived here from the
/// path + structure family so they are uniform across the catalog and
/// in-memory adapters: a container advertises the default child sort (matching
/// the metadata endpoint), a leaf carries none.
fn resource_from_entry(entry: SearchEntry, child_path: &str, base_url: &str) -> Resource {
    let family = entry.structure_family;
    let sorting = match family {
        StructureFamily::Container => Some(default_sorting()),
        _ => None,
    };
    Resource {
        id: entry.key,
        attributes: NodeAttributes {
            ancestors: ancestors_from_path(child_path),
            structure_family: Some(family),
            specs: Some(entry.specs),
            metadata: Some(entry.metadata),
            structure: entry.structure,
            access_blob: entry.access_blob,
            sorting,
            // Populated by the adapter only when the request set
            // `include_data_sources`; `None` otherwise (omitted on the wire).
            data_sources: entry.data_sources,
        },
        links: links::links_for_node(family, base_url, child_path),
    }
}

#[cfg(test)]
mod table_column_tests {
    use std::sync::Arc;

    use arrow::array::{
        BooleanArray, Date32Array, Date64Array, Float64Array, Int64Array, RecordBatch, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use indexmap::IndexMap;

    use super::*;
    use crate::adapters::MapAdapter;
    use crate::core::adapters::{BaseAdapter, BoxFuture};
    use crate::core::dtype::ArrowTable;
    use crate::core::error::Result as CoreResult;
    use crate::core::ndslice::NDSlice;
    use crate::core::structures::{Spec, TableStructure};

    /// A minimal in-memory table adapter over a fixed [`ArrowTable`], enough to
    /// exercise the synthesized column-array hop. `read` honors a one-column
    /// projection exactly like the real file-backed table adapters.
    struct InMemTable {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        structure: TableStructure,
        metadata: serde_json::Value,
        specs: Vec<Spec>,
    }

    impl InMemTable {
        fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
            let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
            let structure = TableStructure {
                arrow_schema: TableStructure::encode_arrow_schema_bytes(&[]),
                npartitions: batches.len().max(1),
                columns,
                resizable: Default::default(),
            };
            Self {
                schema,
                batches,
                structure,
                metadata: serde_json::json!({}),
                specs: vec![],
            }
        }

        fn project(&self, fields: Option<&[String]>) -> CoreResult<ArrowTable> {
            let Some(cols) = fields else {
                return Ok(ArrowTable::new(self.batches.clone(), self.schema.clone()));
            };
            let indices: Vec<usize> = cols
                .iter()
                .map(|name| {
                    self.schema
                        .fields()
                        .iter()
                        .position(|f| f.name() == name)
                        .ok_or_else(|| {
                            crate::core::error::TiledError::Validation(format!(
                                "unknown column: {name}"
                            ))
                        })
                })
                .collect::<CoreResult<Vec<_>>>()?;
            let projected_schema = Arc::new(self.schema.project(&indices).unwrap());
            let batches = self
                .batches
                .iter()
                .map(|b| b.project(&indices).unwrap())
                .collect();
            Ok(ArrowTable::new(batches, projected_schema))
        }
    }

    impl BaseAdapter for InMemTable {
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

    impl TableAdapterRead for InMemTable {
        fn structure(&self) -> &TableStructure {
            &self.structure
        }
        fn read<'a>(
            &'a self,
            fields: Option<&'a [String]>,
        ) -> BoxFuture<'a, CoreResult<ArrowTable>> {
            Box::pin(async move { self.project(fields) })
        }
        fn read_partition<'a>(
            &'a self,
            partition: usize,
            fields: Option<&'a [String]>,
        ) -> BoxFuture<'a, CoreResult<ArrowTable>> {
            Box::pin(async move {
                let b = self
                    .batches
                    .get(partition)
                    .ok_or_else(|| {
                        crate::core::error::TiledError::Validation("partition out of range".into())
                    })?
                    .clone();
                let one = InMemTable {
                    schema: self.schema.clone(),
                    batches: vec![b],
                    structure: self.structure.clone(),
                    metadata: self.metadata.clone(),
                    specs: self.specs.clone(),
                };
                one.project(fields)
            })
        }
    }

    /// Table with columns `x: Int64`, `y: Float64` (with a null), `flag: Boolean`,
    /// split across two partitions (rows [1,2] then [3]) so column reads exercise
    /// the cross-partition concat.
    fn three_col_table() -> InMemTable {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Float64, true),
            Field::new("flag", DataType::Boolean, false),
        ]));
        let b0 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Float64Array::from(vec![Some(1.5), None])),
                Arc::new(BooleanArray::from(vec![true, false])),
            ],
        )
        .unwrap();
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3])),
                Arc::new(Float64Array::from(vec![3.5])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
        )
        .unwrap();
        InMemTable::new(schema, vec![b0, b1])
    }

    fn f64s(bytes: &[u8]) -> Vec<f64> {
        bytes
            .chunks_exact(8)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            .collect()
    }
    fn i64s(bytes: &[u8]) -> Vec<i64> {
        bytes
            .chunks_exact(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect()
    }

    #[test]
    fn datatype_to_builtin_covers_numeric_and_bool() {
        let i64 = arrow_datatype_to_builtin(&DataType::Int64, "x").unwrap();
        assert_eq!(i64.to_numpy_str(), "<i8");
        let f64 = arrow_datatype_to_builtin(&DataType::Float64, "y").unwrap();
        assert_eq!(f64.to_numpy_str(), "<f8");
        let u8 = arrow_datatype_to_builtin(&DataType::UInt8, "u").unwrap();
        assert_eq!(u8.to_numpy_str(), "|u1");
        let b = arrow_datatype_to_builtin(&DataType::Boolean, "flag").unwrap();
        assert_eq!(b.to_numpy_str(), "|b1");
        // A non-numeric column is rejected (WrongType → 404).
        let err = arrow_datatype_to_builtin(&DataType::Utf8, "s").unwrap_err();
        assert!(matches!(err, ServerError::WrongType(_)), "{err:?}");
    }

    #[tokio::test]
    async fn column_int_concatenates_partitions() {
        let table = three_col_table();
        let projected = table.read(Some(&["x".to_string()])).await.unwrap();
        let (data, dtype, nrows) = arrow_column_to_numpy(&projected, "x").unwrap();
        assert_eq!(nrows, 3);
        assert_eq!(dtype.to_numpy_str(), "<i8");
        assert_eq!(i64s(&data), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn column_float_null_becomes_nan() {
        let table = three_col_table();
        let projected = table.read(Some(&["y".to_string()])).await.unwrap();
        let (data, dtype, nrows) = arrow_column_to_numpy(&projected, "y").unwrap();
        assert_eq!(nrows, 3);
        assert_eq!(dtype.to_numpy_str(), "<f8");
        let vals = f64s(&data);
        assert_eq!(vals[0], 1.5);
        assert!(vals[1].is_nan(), "null slot → NaN");
        assert_eq!(vals[2], 3.5);
    }

    #[tokio::test]
    async fn column_bool_expands_to_u8() {
        let table = three_col_table();
        let projected = table.read(Some(&["flag".to_string()])).await.unwrap();
        let (data, dtype, nrows) = arrow_column_to_numpy(&projected, "flag").unwrap();
        assert_eq!(nrows, 3);
        assert_eq!(dtype.to_numpy_str(), "|b1");
        assert_eq!(data.as_ref(), &[1u8, 0, 1]);
    }

    #[tokio::test]
    async fn column_empty_table_zero_rows() {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let table = InMemTable::new(schema, vec![]);
        let projected = table.read(Some(&["x".to_string()])).await.unwrap();
        let (data, dtype, nrows) = arrow_column_to_numpy(&projected, "x").unwrap();
        assert_eq!(nrows, 0);
        assert!(data.is_empty());
        // dtype still comes from the schema field even with no rows.
        assert_eq!(dtype.to_numpy_str(), "<i8");
    }

    /// Decode a fixed-width UTF-32-LE `<U` buffer into per-row Strings, trimming
    /// the trailing U+0000 pad numpy writes for values shorter than the width.
    fn utf32_rows(bytes: &[u8], itemsize: usize) -> Vec<String> {
        if itemsize == 0 {
            return vec![];
        }
        bytes
            .chunks_exact(itemsize)
            .map(|cell| {
                cell.chunks_exact(4)
                    .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
                    .take_while(|&cp| cp != 0)
                    .filter_map(char::from_u32)
                    .collect::<String>()
            })
            .collect()
    }

    /// Two-partition string table: `s` (non-null) whose longest value lives in
    /// the SECOND partition, and `sn` (nullable) carrying one null.
    fn string_table() -> InMemTable {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("sn", DataType::Utf8, true),
        ]));
        let b0 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "bb"])),
                Arc::new(StringArray::from(vec![Some("x"), None])),
            ],
        )
        .unwrap();
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["cccc"])),
                Arc::new(StringArray::from(vec![Some("y")])),
            ],
        )
        .unwrap();
        InMemTable::new(schema, vec![b0, b1])
    }

    #[tokio::test]
    async fn column_string_is_fixed_width_unicode_over_all_partitions() {
        let table = string_table();
        let projected = table.read(Some(&["s".to_string()])).await.unwrap();
        let (data, dtype, nrows) = arrow_column_to_numpy(&projected, "s").unwrap();
        assert_eq!(nrows, 3);
        // Width = longest value ("cccc", in partition 2) = 4 chars → `<U4`.
        assert_eq!(dtype.to_numpy_str(), "<U4");
        assert_eq!(dtype.element_size(), 16);
        assert_eq!(utf32_rows(&data, 16), vec!["a", "bb", "cccc"]);
    }

    #[tokio::test]
    async fn column_null_string_renders_as_none_literal() {
        let table = string_table();
        let projected = table.read(Some(&["sn".to_string()])).await.unwrap();
        let (data, dtype, nrows) = arrow_column_to_numpy(&projected, "sn").unwrap();
        assert_eq!(nrows, 3);
        // The null becomes "None" (4 chars), the longest value → `<U4`.
        assert_eq!(dtype.to_numpy_str(), "<U4");
        assert_eq!(utf32_rows(&data, 16), vec!["x", "None", "y"]);
    }

    #[tokio::test]
    async fn column_empty_string_is_u0() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let table = InMemTable::new(schema, vec![]);
        let projected = table.read(Some(&["s".to_string()])).await.unwrap();
        let (data, dtype, nrows) = arrow_column_to_numpy(&projected, "s").unwrap();
        assert_eq!(nrows, 0);
        assert!(data.is_empty());
        assert_eq!(dtype.to_numpy_str(), "<U0");
    }

    #[test]
    fn datatype_to_builtin_maps_temporal_to_datetime64() {
        let ny = Some("America/New_York".into());
        let cases: &[(DataType, &str)] = &[
            (DataType::Timestamp(TimeUnit::Second, None), "<M8[s]"),
            (DataType::Timestamp(TimeUnit::Millisecond, None), "<M8[ms]"),
            // A timezone is dropped: numpy `datetime64` is tz-naive.
            (DataType::Timestamp(TimeUnit::Microsecond, ny), "<M8[us]"),
            (DataType::Timestamp(TimeUnit::Nanosecond, None), "<M8[ns]"),
            (DataType::Date32, "<M8[D]"),
            (DataType::Date64, "<M8[ms]"),
        ];
        for (dt, expected) in cases {
            let got = arrow_datatype_to_builtin(dt, "t").unwrap();
            assert_eq!(got.to_numpy_str(), *expected, "{dt:?}");
            assert_eq!(got.element_size(), 8, "datetime64 is always 8 bytes");
        }
    }

    /// Two-partition timestamp table (`ts: Timestamp(ms)`, nullable), the null in
    /// partition 0 and a value in partition 1, so ticks exercise the concat and
    /// the `NaT` sentinel.
    fn timestamp_table() -> InMemTable {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        let b0 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(TimestampMillisecondArray::from(vec![
                Some(1000),
                None,
            ]))],
        )
        .unwrap();
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(TimestampMillisecondArray::from(vec![Some(3000)]))],
        )
        .unwrap();
        InMemTable::new(schema, vec![b0, b1])
    }

    #[tokio::test]
    async fn column_timestamp_ms_null_is_nat() {
        let table = timestamp_table();
        let projected = table.read(Some(&["ts".to_string()])).await.unwrap();
        let (data, dtype, nrows) = arrow_column_to_numpy(&projected, "ts").unwrap();
        assert_eq!(nrows, 3);
        assert_eq!(dtype.to_numpy_str(), "<M8[ms]");
        // Null → i64::MIN (numpy `NaT`).
        assert_eq!(i64s(&data), vec![1000, i64::MIN, 3000]);
    }

    #[tokio::test]
    async fn column_tz_aware_timestamp_serves_naive_utc_ticks() {
        // Arrow stores a tz-aware timestamp as UTC ticks + a tz annotation.
        // Dropping the tz yields those same ticks under a tz-naive `<M8[us]`.
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into())),
            false,
        )]));
        let arr = TimestampMicrosecondArray::from(vec![1_000_000, 2_000_000])
            .with_timezone("America/New_York");
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        let table = InMemTable::new(schema, vec![batch]);
        let projected = table.read(Some(&["ts".to_string()])).await.unwrap();
        let (data, dtype, nrows) = arrow_column_to_numpy(&projected, "ts").unwrap();
        assert_eq!(nrows, 2);
        // tz dropped: unit is [us], no tz surfaced.
        assert_eq!(dtype.to_numpy_str(), "<M8[us]");
        assert_eq!(dtype.dt_units.as_deref(), Some("[us]"));
        assert_eq!(i64s(&data), vec![1_000_000, 2_000_000]);
    }

    #[tokio::test]
    async fn column_date32_is_day_ticks() {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Date32Array::from(vec![Some(10), None, Some(20)]))],
        )
        .unwrap();
        let table = InMemTable::new(schema, vec![batch]);
        let projected = table.read(Some(&["d".to_string()])).await.unwrap();
        let (data, dtype, nrows) = arrow_column_to_numpy(&projected, "d").unwrap();
        assert_eq!(nrows, 3);
        // Date32 → day-unit datetime64; i32 days widened to i64, null → NaT.
        assert_eq!(dtype.to_numpy_str(), "<M8[D]");
        assert_eq!(i64s(&data), vec![10, i64::MIN, 20]);
    }

    #[tokio::test]
    async fn column_date64_is_ms_ticks() {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("d", DataType::Date64, false)]));
        // 86_400_000 ms = 1 day.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Date64Array::from(vec![0, 86_400_000]))],
        )
        .unwrap();
        let table = InMemTable::new(schema, vec![batch]);
        let projected = table.read(Some(&["d".to_string()])).await.unwrap();
        let (data, dtype, nrows) = arrow_column_to_numpy(&projected, "d").unwrap();
        assert_eq!(nrows, 2);
        assert_eq!(dtype.to_numpy_str(), "<M8[ms]");
        assert_eq!(i64s(&data), vec![0, 86_400_000]);
    }

    fn root_with_table() -> Arc<dyn ContainerAdapter> {
        let mut mapping = IndexMap::new();
        mapping.insert(
            "tbl".to_string(),
            AnyAdapter::Table(Arc::new(three_col_table())),
        );
        Arc::new(MapAdapter::new(mapping, serde_json::json!({}), vec![])) as _
    }

    #[tokio::test]
    async fn walk_table_column_yields_array_view() {
        let root = root_with_table();
        let seg = |s: &[&str]| s.iter().map(|x| x.to_string()).collect::<Vec<_>>();

        // [tbl, y] resolves to a synthesized array node.
        let node = walk_tree(root.as_ref(), &seg(&["tbl", "y"])).await.unwrap();
        assert_eq!(node.structure_family(), StructureFamily::Array);
        let arr = node.as_array_arc().expect("column resolves to an array");
        assert_eq!(arr.structure().shape, vec![3]);
        assert_eq!(arr.structure().chunks, vec![vec![3]]);
        assert_eq!(arr.structure().data_type.element_size(), 8);
        // Its full read returns the column values (null → NaN).
        let data = arr.read(&NDSlice::empty()).await.unwrap();
        let vals = f64s(&data.data);
        assert_eq!(vals[0], 1.5);
        assert!(vals[1].is_nan());
        assert_eq!(vals[2], 3.5);
    }

    #[tokio::test]
    async fn walk_table_missing_column_is_404() {
        let root = root_with_table();
        let seg = |s: &[&str]| s.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let err = walk_tree(root.as_ref(), &seg(&["tbl", "nope"]))
            .await
            .err()
            .expect("missing column must error");
        assert!(matches!(err, ServerError::NotFound(_)), "{err:?}");
    }

    #[tokio::test]
    async fn walk_bare_table_is_still_a_table() {
        let root = root_with_table();
        let seg = |s: &[&str]| s.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let node = walk_tree(root.as_ref(), &seg(&["tbl"])).await.unwrap();
        assert_eq!(node.structure_family(), StructureFamily::Table);
    }

    #[tokio::test]
    async fn walk_past_column_cannot_descend() {
        // A column is an array leaf: [tbl, y, extra] must not resolve.
        let root = root_with_table();
        let seg = |s: &[&str]| s.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let err = walk_tree(root.as_ref(), &seg(&["tbl", "y", "extra"]))
            .await
            .err()
            .expect("descending past a column must error");
        assert!(matches!(err, ServerError::NotFound(_)), "{err:?}");
    }
}
