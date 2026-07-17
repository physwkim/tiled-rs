//! Integration tests for walking a table node by column name — the Rust port
//! of upstream tiled's behavior where a table/dataframe column is addressable as
//! a child array node (`TableAdapter.__getitem__` → `ArrayAdapter.from_array`,
//! adapters/table.py:137-146; the zarr router lists columns as group members,
//! server/zarr.py:209-214).
//!
//! Drives the real HTTP surface (tower `oneshot`) against an in-memory
//! `MapAdapter` tree holding a two-partition Arrow-IPC table with columns
//! `x: Int64`, `y: Float64` (one null), `flag: Boolean`. Covers the metadata
//! route, `/array/full`, `/array/block`, the `/zarr/v2` + `/zarr/v3` column
//! reads, and 404 for a nonexistent column.

#![cfg(feature = "arrow-ipc")]

use std::sync::Arc;

use arrow::array::{
    BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::FileWriter;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use indexmap::IndexMap;
use tower::ServiceExt;

use tiled_rs::adapters::{ArrowIpcAdapter, MapAdapter};
use tiled_rs::core::adapters::{AnyAdapter, ContainerAdapter};

/// Write one Arrow IPC file (one partition) with the fixed 3-column schema.
fn write_partition(path: &std::path::Path, xs: Vec<i64>, ys: Vec<Option<f64>>, flags: Vec<bool>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("y", DataType::Float64, true),
        Field::new("flag", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(xs)),
            Arc::new(Float64Array::from(ys)),
            Arc::new(BooleanArray::from(flags)),
        ],
    )
    .unwrap();
    let f = std::fs::File::create(path).unwrap();
    let mut w = FileWriter::try_new(f, &schema).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
}

/// Build an app whose root holds a single two-partition table `some_table`
/// (rows [1,2] then [3]). Returns the app plus the tempdir (kept alive so the
/// backing `.arrow` files survive for the request).
fn build_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let p0 = dir.path().join("p0.arrow");
    let p1 = dir.path().join("p1.arrow");
    write_partition(&p0, vec![1, 2], vec![Some(1.5), None], vec![true, false]);
    write_partition(&p1, vec![3], vec![Some(3.5)], vec![true]);

    let table = ArrowIpcAdapter::from_paths(vec![p0, p1], serde_json::json!({})).unwrap();
    let root = single_table_root("some_table", AnyAdapter::Table(Arc::new(table)));
    (app_for_root(root), dir)
}

/// Wrap one table under a `MapAdapter` root keyed by `name`.
fn single_table_root(name: &str, table: AnyAdapter) -> Arc<dyn ContainerAdapter> {
    let mut mapping = IndexMap::new();
    mapping.insert(name.to_string(), table);
    Arc::new(MapAdapter::new(mapping, serde_json::json!({}), vec![]))
}

/// Build the HTTP app over a given root tree (all AppState knobs at defaults).
fn app_for_root(root: Arc<dyn ContainerAdapter>) -> axum::Router {
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree: root,
        serialization_registry: registry,
        query_names: vec![],
        base_url: Some("http://localhost:8000".to_string()),
        cors_policy: tiled_rs::server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: None,
        auth_db: None,
        issuer: None,
        authenticators: vec![],
        proxied_header_auth: None,
        external_oidc: None,
        #[cfg(feature = "saml")]
        saml_providers: vec![],
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_cache: tiled_rs::server::streaming_cache::disabled(),
        access_policy: None,
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
        enable_web: false,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        webhook_dispatcher: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    };
    tiled_rs::server::build_app(state)
}

/// Write one Arrow IPC file holding a single nullable Utf8 column `name`.
fn write_string_partition(path: &std::path::Path, names: Vec<Option<&str>>) {
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(names))]).unwrap();
    let f = std::fs::File::create(path).unwrap();
    let mut w = FileWriter::try_new(f, &schema).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
}

/// App whose root holds a two-partition table `str_table` with one nullable
/// string column `name`: rows `["al", null]` then `["charlie"]`. The longest
/// value ("charlie", 7 chars) lives in the SECOND partition, and the null
/// renders as the literal "None".
fn build_string_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let p0 = dir.path().join("s0.arrow");
    let p1 = dir.path().join("s1.arrow");
    write_string_partition(&p0, vec![Some("al"), None]);
    write_string_partition(&p1, vec![Some("charlie")]);

    let table = ArrowIpcAdapter::from_paths(vec![p0, p1], serde_json::json!({})).unwrap();
    let root = single_table_root("str_table", AnyAdapter::Table(Arc::new(table)));
    (app_for_root(root), dir)
}

/// Write one Arrow IPC file holding a single nullable `Timestamp(ms)` column
/// `t`.
fn write_timestamp_partition(path: &std::path::Path, ticks: Vec<Option<i64>>) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(TimestampMillisecondArray::from(ticks))],
    )
    .unwrap();
    let f = std::fs::File::create(path).unwrap();
    let mut w = FileWriter::try_new(f, &schema).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
}

/// App whose root holds a two-partition table `ts_table` with one nullable
/// `Timestamp(ms)` column `t`: ticks `[1000, null]` then `[3000]`. The null
/// becomes numpy `NaT` (`i64::MIN`); the unit is `<M8[ms]` (8-byte int64 ticks).
fn build_temporal_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let p0 = dir.path().join("t0.arrow");
    let p1 = dir.path().join("t1.arrow");
    write_timestamp_partition(&p0, vec![Some(1000), None]);
    write_timestamp_partition(&p1, vec![Some(3000)]);

    let table = ArrowIpcAdapter::from_paths(vec![p0, p1], serde_json::json!({})).unwrap();
    let root = single_table_root("ts_table", AnyAdapter::Table(Arc::new(table)));
    (app_for_root(root), dir)
}

/// Decode a fixed-width UTF-32-LE `<U` buffer into per-row Strings.
fn utf32_rows(bytes: &[u8], itemsize: usize) -> Vec<String> {
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

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

async fn get_json(app: &axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let (status, bytes) = get(app, uri).await;
    let v = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
}

fn i64s(bytes: &[u8]) -> Vec<i64> {
    bytes
        .chunks_exact(8)
        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
        .collect()
}
fn f64s(bytes: &[u8]) -> Vec<f64> {
    bytes
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

// ---------------------------------------------------------------------------
// metadata route
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metadata_of_column_is_an_array_with_schema_dtype_and_shape() {
    let (app, _dir) = build_app();

    // Int64 column `x`.
    let (status, body) = get_json(&app, "/api/v1/metadata/some_table/x").await;
    assert_eq!(status, StatusCode::OK);
    let attrs = &body["data"]["attributes"];
    assert_eq!(attrs["structure_family"], "array");
    assert_eq!(body["data"]["id"], "x");
    // ancestors = ["some_table"] (the table node).
    assert_eq!(
        attrs["ancestors"],
        serde_json::json!(["some_table"]),
        "column node's ancestor is the table"
    );
    let structure = &attrs["structure"];
    assert_eq!(structure["shape"], serde_json::json!([3]), "3 rows");
    assert_eq!(
        structure["chunks"],
        serde_json::json!([[3]]),
        "single-chunk column"
    );
    // dtype comes from the Arrow schema: signed 8-byte integer.
    assert_eq!(structure["data_type"]["kind"], "i");
    assert_eq!(structure["data_type"]["itemsize"], 8);

    // Float64 column `y`.
    let (status, body) = get_json(&app, "/api/v1/metadata/some_table/y").await;
    assert_eq!(status, StatusCode::OK);
    let structure = &body["data"]["attributes"]["structure"];
    assert_eq!(structure["data_type"]["kind"], "f");
    assert_eq!(structure["data_type"]["itemsize"], 8);
    assert_eq!(structure["shape"], serde_json::json!([3]));
}

#[tokio::test]
async fn metadata_of_missing_column_is_404() {
    let (app, _dir) = build_app();
    let (status, _) = get(&app, "/api/v1/metadata/some_table/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// /array/full + /array/block
// ---------------------------------------------------------------------------

#[tokio::test]
async fn array_full_reads_column_across_partitions() {
    let (app, _dir) = build_app();

    // Int64 column concatenated across both partitions → [1, 2, 3].
    let (status, body) = get(&app, "/api/v1/array/full/some_table/x").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(i64s(&body), vec![1, 2, 3]);

    // Float64 column: the null slot in partition 0 becomes NaN.
    let (status, body) = get(&app, "/api/v1/array/full/some_table/y").await;
    assert_eq!(status, StatusCode::OK);
    let ys = f64s(&body);
    assert_eq!(ys[0], 1.5);
    assert!(ys[1].is_nan(), "arrow null → NaN");
    assert_eq!(ys[2], 3.5);
}

#[tokio::test]
async fn array_block_reads_column_chunk() {
    let (app, _dir) = build_app();
    // The whole column is a single chunk, so block 0 returns every row.
    let (status, body) = get(&app, "/api/v1/array/block/some_table/x?block=0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(i64s(&body), vec![1, 2, 3]);
}

#[tokio::test]
async fn array_full_of_missing_column_is_404() {
    let (app, _dir) = build_app();
    let (status, _) = get(&app, "/api/v1/array/full/some_table/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// /zarr/v2
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zarr_v2_table_lists_columns_and_serves_them() {
    let (app, _dir) = build_app();

    // The table is a zarr group.
    let (status, body) = get_json(&app, "/zarr/v2/some_table/.zgroup").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!({"zarr_format": 2}));

    // The group listing enumerates the column names as child URLs.
    let (status, body) = get_json(&app, "/zarr/v2/some_table").await;
    assert_eq!(status, StatusCode::OK);
    let urls: Vec<String> = serde_json::from_value(body).unwrap();
    assert_eq!(urls.len(), 3);
    assert!(urls[0].ends_with("/zarr/v2/some_table/x"), "{urls:?}");
    assert!(urls[1].ends_with("/zarr/v2/some_table/y"), "{urls:?}");
    assert!(urls[2].ends_with("/zarr/v2/some_table/flag"), "{urls:?}");

    // A column resolves to a zarr array with the schema dtype.
    let (status, doc) = get_json(&app, "/zarr/v2/some_table/x/.zarray").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["zarr_format"], 2);
    assert_eq!(doc["dtype"], "<i8");
    assert_eq!(doc["shape"], serde_json::json!([3]));
    assert_eq!(doc["chunks"], serde_json::json!([3]));

    // Its chunk bytes are the column values.
    let (status, chunk) = get(&app, "/zarr/v2/some_table/x/0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(i64s(&chunk), vec![1, 2, 3]);
}

#[tokio::test]
async fn zarr_v2_missing_column_is_404() {
    let (app, _dir) = build_app();
    let (status, _) = get(&app, "/zarr/v2/some_table/nope/.zarray").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// /zarr/v3
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zarr_v3_table_group_and_column_array() {
    let (app, _dir) = build_app();

    // The table is a v3 group.
    let (status, doc) = get_json(&app, "/zarr/v3/some_table/zarr.json").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["node_type"], "group");

    // The group listing enumerates the columns.
    let (status, body) = get_json(&app, "/zarr/v3/some_table").await;
    assert_eq!(status, StatusCode::OK);
    let urls: Vec<String> = serde_json::from_value(body).unwrap();
    assert_eq!(urls.len(), 3);
    assert!(urls[0].ends_with("/zarr/v3/some_table/x"), "{urls:?}");

    // A column resolves to a v3 array document with the core dtype name.
    let (status, doc) = get_json(&app, "/zarr/v3/some_table/x/zarr.json").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["node_type"], "array");
    assert_eq!(doc["data_type"], "int64");
    assert_eq!(doc["shape"], serde_json::json!([3]));

    // Its chunk bytes (v3 key `c/0`) are the column values.
    let (status, chunk) = get(&app, "/zarr/v3/some_table/x/c/0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(i64s(&chunk), vec![1, 2, 3]);
}

#[tokio::test]
async fn zarr_v3_missing_column_is_404() {
    let (app, _dir) = build_app();
    let (status, _) = get(&app, "/zarr/v3/some_table/nope/zarr.json").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// string columns (Utf8 → fixed-width UTF-32-LE `<U{n}`)
//
// Parity: upstream coerces an object/string column to a numpy `<U` array via
// `numpy.array([str(x) for x in array])` (adapters/array.py:73-78; null → the
// literal "None"). The fixed width is the longest value over the WHOLE
// concatenated column, so here "charlie" (7 chars) in the second partition sets
// `<U7` (itemsize 28), even though partition 0's longest is only 2 chars.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metadata_of_string_column_is_fixed_width_unicode() {
    let (app, _dir) = build_string_app();

    let (status, body) = get_json(&app, "/api/v1/metadata/str_table/name").await;
    assert_eq!(status, StatusCode::OK);
    let attrs = &body["data"]["attributes"];
    assert_eq!(attrs["structure_family"], "array");
    let structure = &attrs["structure"];
    assert_eq!(structure["shape"], serde_json::json!([3]), "3 rows");
    // `<U7`: unicode kind, itemsize = 4 * longest-char-count (7) = 28 bytes.
    assert_eq!(structure["data_type"]["kind"], "U");
    assert_eq!(
        structure["data_type"]["itemsize"], 28,
        "width is the longest value over ALL partitions (charlie=7 → 28)"
    );
}

#[tokio::test]
async fn array_full_of_string_column_octet_stream_roundtrip() {
    let (app, _dir) = build_string_app();

    let (status, body) = get(&app, "/api/v1/array/full/str_table/name").await;
    assert_eq!(status, StatusCode::OK);
    // 3 rows × 28 bytes (7 code points × 4) = 84 bytes.
    assert_eq!(body.len(), 84, "3 rows × <U7 itemsize 28");
    assert_eq!(
        utf32_rows(&body, 28),
        vec!["al".to_string(), "None".to_string(), "charlie".to_string()],
        "null renders as the literal \"None\" (array.py:78)"
    );
}

#[tokio::test]
async fn csv_of_string_column_renders_cell_text() {
    let (app, _dir) = build_string_app();

    let (status, body) = get(&app, "/api/v1/array/full/str_table/name?format=text/csv").await;
    assert_eq!(status, StatusCode::OK);
    // 1-D array → one cell per line, no trailing newline; null → "None".
    assert_eq!(String::from_utf8(body).unwrap(), "al\nNone\ncharlie");
}

#[tokio::test]
async fn zarr_v2_string_column_zarray_and_chunk() {
    let (app, _dir) = build_string_app();

    // The column resolves to a zarr v2 array whose dtype is the numpy `<U7`.
    let (status, doc) = get_json(&app, "/zarr/v2/str_table/name/.zarray").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["zarr_format"], 2);
    assert_eq!(doc["dtype"], "<U7");
    assert_eq!(doc["shape"], serde_json::json!([3]));
    assert_eq!(doc["chunks"], serde_json::json!([3]));

    // Its chunk bytes decode back to the padded string cells.
    let (status, chunk) = get(&app, "/zarr/v2/str_table/name/0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(chunk.len(), 84);
    assert_eq!(
        utf32_rows(&chunk, 28),
        vec!["al".to_string(), "None".to_string(), "charlie".to_string()]
    );
}

#[tokio::test]
async fn zarr_v3_string_column_is_422() {
    // Parity ceiling: zarr v3 has no fixed-width unicode data type, so upstream
    // (and this port) reject a string column with 422 rather than inventing one.
    let (app, _dir) = build_string_app();
    let (status, _) = get(&app, "/zarr/v3/str_table/name/zarr.json").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

// ---------------------------------------------------------------------------
// temporal columns (Timestamp/Date → datetime64 `<M8[unit]`)
//
// Parity: upstream serves a datetime column as numpy `datetime64`, an 8-byte
// int64 tick count under a unit-tagged dtype. A null is `NaT` (`i64::MIN`). A
// tz-aware timestamp becomes tz-naive UTC ticks (numpy `datetime64` carries no
// tz). zarr v3 has no datetime64 core data type — a parity ceiling, 422.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metadata_of_timestamp_column_is_datetime64() {
    let (app, _dir) = build_temporal_app();

    let (status, body) = get_json(&app, "/api/v1/metadata/ts_table/t").await;
    assert_eq!(status, StatusCode::OK);
    let structure = &body["data"]["attributes"]["structure"];
    assert_eq!(structure["shape"], serde_json::json!([3]), "3 rows");
    let dt = &structure["data_type"];
    assert_eq!(dt["kind"], "M", "datetime64");
    assert_eq!(dt["itemsize"], 8);
    assert_eq!(dt["dt_units"], "[ms]", "Timestamp(ms) unit carried through");
}

#[tokio::test]
async fn array_full_of_timestamp_column_is_int64_ticks() {
    let (app, _dir) = build_temporal_app();

    let (status, body) = get(&app, "/api/v1/array/full/ts_table/t").await;
    assert_eq!(status, StatusCode::OK);
    // 3 rows × 8 bytes int64 ticks; the null slot is `NaT` (i64::MIN).
    assert_eq!(i64s(&body), vec![1000, i64::MIN, 3000]);
}

#[tokio::test]
async fn csv_of_timestamp_column_renders_iso_cells() {
    let (app, _dir) = build_temporal_app();

    let (status, body) = get(&app, "/api/v1/array/full/ts_table/t?format=text/csv").await;
    assert_eq!(status, StatusCode::OK);
    // ms unit → `str(numpy.datetime64)` ISO with a 3-digit fraction; NaT → "NaT".
    assert_eq!(
        String::from_utf8(body).unwrap(),
        "1970-01-01T00:00:01.000\nNaT\n1970-01-01T00:00:03.000"
    );
}

#[tokio::test]
async fn zarr_v2_timestamp_column_zarray_and_chunk() {
    let (app, _dir) = build_temporal_app();

    let (status, doc) = get_json(&app, "/zarr/v2/ts_table/t/.zarray").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["zarr_format"], 2);
    assert_eq!(doc["dtype"], "<M8[ms]");
    assert_eq!(doc["shape"], serde_json::json!([3]));

    // Its chunk bytes are the raw int64 ticks.
    let (status, chunk) = get(&app, "/zarr/v2/ts_table/t/0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(i64s(&chunk), vec![1000, i64::MIN, 3000]);
}

#[tokio::test]
async fn zarr_v3_timestamp_column_is_422() {
    // Parity ceiling: zarr v3 has no datetime64 core data type.
    let (app, _dir) = build_temporal_app();
    let (status, _) = get(&app, "/zarr/v3/ts_table/t/zarr.json").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}
