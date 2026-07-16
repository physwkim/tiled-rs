//! Core response construction logic.
//!
//! Corresponds to `tiled/server/core.py` — `construct_resource`, `construct_entries_response`.

use std::sync::Arc;

use crate::core::adapters::{AnyAdapter, ContainerAdapter, SearchEntry, TableAdapterRead};
use crate::core::dtype::{ArrowTable, BuiltinDType, Endianness, Kind};
use crate::core::links;
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

/// Map an Arrow numeric/boolean [`arrow::datatypes::DataType`] to the numpy
/// [`BuiltinDType`] a column-array view exposes. One-byte types use numpy's
/// "not applicable" byte-order marker (`|i1`, `|u1`, `|b1`); multi-byte numerics
/// are little-endian (the byte order [`arrow_array_to_le_bytes`] emits). A
/// non-numeric column (string, temporal, nested) is rejected — the array routes
/// cannot serve it.
fn arrow_datatype_to_builtin(
    dt: &arrow::datatypes::DataType,
    column: &str,
) -> Result<BuiltinDType, ServerError> {
    use arrow::datatypes::DataType;
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
        Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::DataType;

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

/// Construct a Resource for a given adapter.
///
/// Async because a container node's `structure_json` now awaits a child count
/// (a DB `count_children` for the SQL catalog).
pub async fn construct_resource(
    adapter: &AnyAdapter,
    id: &str,
    path: &str,
    base_url: &str,
) -> Result<Resource, ServerError> {
    let family = adapter.structure_family();
    let node_links = links::links_for_node(family, base_url, path);

    let sorting = match adapter {
        AnyAdapter::Container(_) => Some(default_sorting()),
        _ => None,
    };

    Ok(Resource {
        id: id.to_string(),
        attributes: NodeAttributes {
            ancestors: ancestors_from_path(path),
            structure_family: Some(family),
            specs: Some(adapter.specs().to_vec()),
            metadata: Some(adapter.metadata().clone()),
            structure: adapter.structure_json().await?,
            access_blob: None,
            sorting,
            data_sources: None,
        },
        links: node_links,
    })
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
        resources.push(resource_from_entry(entry, &child_path, base_url));
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

    use arrow::array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
