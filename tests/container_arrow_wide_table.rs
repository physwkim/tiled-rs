//! Wide-table export for `xarray_dataset` containers — the server
//! `/container/full` route and its POST field-list fallback, across every format
//! the wide table can be encoded as: arrow, csv, and parquet.
//!
//! Upstream serves a Container in these formats via *spec*-keyed serializers
//! (`serialization/xarray.py:68/73/80`, registered under the `xarray_dataset`
//! spec, each deriving from one `to_dataframe()`); the Rust serialization registry
//! keys on `StructureFamily` only, so the route mirrors that logic inline, gated on
//! the spec, and delegates the per-format encode to the TABLE-family serializer.
//! `field=` (repeated GET query, upstream `router.py:1352`) / a JSON-array POST
//! body (upstream `router.py:1396`) select the column projection.
//!
//! `application/json`/`text/html` are NOT covered here: the Container family
//! already serves those, so honoring the `xarray_dataset` json/html serializers
//! would override the container default — the spec-before-family dispatch (P8),
//! blocked on sign-off.
//!
//! These cases drive the HTTP surface directly (tower `oneshot`). The client
//! `DatasetClient` path that drives this route end to end lives in
//! `xarray_wide_table_client.rs`.

#![cfg(feature = "arrow-ipc")]

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array};
use arrow::ipc::reader::FileReader;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use bytes::Bytes;
use indexmap::IndexMap;
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::adapters::{ArrayAdapter, MapAdapter};
use tiled_rs::core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_rs::core::dtype::{BuiltinDType, Endianness, Kind};
use tiled_rs::core::structures::Spec;

const ARROW_MIME: &str = "application/vnd.apache.arrow.file";

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

/// A 1-D `f64` array child carrying `spec` (e.g. `xarray_coord`) and `metadata`.
fn f64_var(data: &[f64], spec: &str, metadata: serde_json::Value) -> AnyAdapter {
    let bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
    let dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
    let arr = ArrayAdapter::from_array(
        Bytes::from(bytes),
        dtype,
        vec![data.len()],
        vec![vec![data.len()]],
        metadata,
        vec![Spec::new(spec)],
    );
    AnyAdapter::Array(Arc::new(arr))
}

/// A 1-D `i64` array child carrying `spec`.
fn i64_var(data: &[i64], spec: &str) -> AnyAdapter {
    let bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
    let dtype = BuiltinDType::new(Endianness::Little, Kind::Integer, 8);
    let arr = ArrayAdapter::from_array(
        Bytes::from(bytes),
        dtype,
        vec![data.len()],
        vec![vec![data.len()]],
        json!({}),
        vec![Spec::new(spec)],
    );
    AnyAdapter::Array(Arc::new(arr))
}

/// A 1-D float16 array child carrying `spec`. Values are given as IEEE-754
/// half-precision bit patterns (little-endian on the wire); the server widens
/// them to f32 for the csv/parquet wide-table export.
fn f16_var(bits: &[u16], spec: &str) -> AnyAdapter {
    let bytes: Vec<u8> = bits.iter().flat_map(|v| v.to_le_bytes()).collect();
    let dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 2);
    let arr = ArrayAdapter::from_array(
        Bytes::from(bytes),
        dtype,
        vec![bits.len()],
        vec![vec![bits.len()]],
        json!({}),
        vec![Spec::new(spec)],
    );
    AnyAdapter::Array(Arc::new(arr))
}

fn container(children: Vec<(&str, AnyAdapter)>, specs: Vec<Spec>) -> AnyAdapter {
    let mut m = IndexMap::new();
    for (k, v) in children {
        m.insert(k.to_string(), v);
    }
    AnyAdapter::Container(Arc::new(MapAdapter::new(m, json!({}), specs)))
}

/// `weather`: an `xarray_dataset` with a coord `time` (f64), a data var `temp`
/// (f64), and a data var `pressure` (i64). All length 3.
fn weather() -> AnyAdapter {
    container(
        vec![
            (
                "time",
                f64_var(&[10.0, 20.0, 30.0], "xarray_coord", json!({})),
            ),
            (
                "temp",
                f64_var(&[1.5, 2.5, 3.5], "xarray_data_var", json!({})),
            ),
            ("pressure", i64_var(&[100, 200, 300], "xarray_data_var")),
        ],
        vec![Spec::new("xarray_dataset")],
    )
}

/// `weather_f16`: an `xarray_dataset` with an f64 coord `time` and a **float16**
/// data var `half` = [0.5, 1.5, 2.5] (exact half-precision bit patterns).
fn weather_f16() -> AnyAdapter {
    container(
        vec![
            (
                "time",
                f64_var(&[10.0, 20.0, 30.0], "xarray_coord", json!({})),
            ),
            // 0.5 = 0x3800, 1.5 = 0x3E00, 2.5 = 0x4100 in IEEE-754 half.
            ("half", f16_var(&[0x3800, 0x3E00, 0x4100], "xarray_data_var")),
        ],
        vec![Spec::new("xarray_dataset")],
    )
}

fn root_with(children: Vec<(&str, AnyAdapter)>) -> Arc<dyn ContainerAdapter> {
    let mut m = IndexMap::new();
    for (k, v) in children {
        m.insert(k.to_string(), v);
    }
    Arc::new(MapAdapter::new(m, json!({}), vec![]))
}

fn app_for_root(root: Arc<dyn ContainerAdapter>) -> axum::Router {
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree: root,
        serialization_registry: registry,
        query_names: tiled_rs::core::queries::Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: Some("http://localhost:8000".to_string()),
        root_path: String::new(),
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
        allow_anonymous_access: false,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
        validation: Default::default(),
    };
    tiled_rs::server::build_app(state)
}

// ---------------------------------------------------------------------------
// HTTP + arrow helpers
// ---------------------------------------------------------------------------

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header("accept", ARROW_MIME)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

async fn post(app: &axum::Router, uri: &str, body: serde_json::Value) -> (StatusCode, Vec<u8>) {
    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/json")
        .header("accept", ARROW_MIME)
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

/// Column names of an Arrow IPC FILE, in schema order.
fn arrow_columns(bytes: &[u8]) -> Vec<String> {
    let reader = FileReader::try_new(Cursor::new(bytes.to_vec()), None).unwrap();
    reader
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

fn arrow_f64(bytes: &[u8], name: &str) -> Vec<f64> {
    let reader = FileReader::try_new(Cursor::new(bytes.to_vec()), None).unwrap();
    let idx = reader.schema().index_of(name).unwrap();
    let mut out = Vec::new();
    for batch in reader {
        let b = batch.unwrap();
        let a = b
            .column(idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        out.extend(a.values().iter().copied());
    }
    out
}

fn arrow_i64(bytes: &[u8], name: &str) -> Vec<i64> {
    let reader = FileReader::try_new(Cursor::new(bytes.to_vec()), None).unwrap();
    let idx = reader.schema().index_of(name).unwrap();
    let mut out = Vec::new();
    for batch in reader {
        let b = batch.unwrap();
        let a = b.column(idx).as_any().downcast_ref::<Int64Array>().unwrap();
        out.extend(a.values().iter().copied());
    }
    out
}

// ---------------------------------------------------------------------------
// GET field projection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_arrow_field_projection_roundtrip() {
    let app = app_for_root(root_with(vec![("weather", weather())]));

    // Project a subset, in requested order: only `time` and `pressure`.
    let (status, body) = get(
        &app,
        "/api/v1/container/full/weather?format=arrow&field=time&field=pressure",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        arrow_columns(&body),
        vec!["time".to_string(), "pressure".to_string()],
        "only the projected columns, in requested order"
    );
    assert_eq!(arrow_f64(&body, "time"), vec![10.0, 20.0, 30.0]);
    assert_eq!(arrow_i64(&body, "pressure"), vec![100, 200, 300]);
}

#[tokio::test]
async fn get_arrow_no_projection_returns_all_children() {
    let app = app_for_root(root_with(vec![("weather", weather())]));

    let (status, body) = get(&app, "/api/v1/container/full/weather?format=arrow").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        arrow_columns(&body),
        vec![
            "time".to_string(),
            "temp".to_string(),
            "pressure".to_string()
        ],
        "no field= → every child, in container order"
    );
    assert_eq!(arrow_f64(&body, "temp"), vec![1.5, 2.5, 3.5]);
}

// The `column` GET key is accepted as an alias for `field` (parity with the
// table route's projection keys).
#[tokio::test]
async fn get_arrow_column_alias_projects() {
    let app = app_for_root(root_with(vec![("weather", weather())]));
    let (status, body) = get(
        &app,
        "/api/v1/container/full/weather?format=arrow&column=temp",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(arrow_columns(&body), vec!["temp".to_string()]);
    assert_eq!(arrow_f64(&body, "temp"), vec![1.5, 2.5, 3.5]);
}

// ---------------------------------------------------------------------------
// POST field-list body fallback
// ---------------------------------------------------------------------------

#[tokio::test]
async fn post_arrow_field_list_body_roundtrip() {
    let app = app_for_root(root_with(vec![("weather", weather())]));

    // The field list moves into a bare JSON-array body; `format` stays a query
    // param. Mirrors the client's >2000-char fallback and upstream `json=field`.
    let (status, body) = post(
        &app,
        "/api/v1/container/full/weather?format=arrow",
        json!(["time", "temp", "pressure"]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        arrow_columns(&body),
        vec![
            "time".to_string(),
            "temp".to_string(),
            "pressure".to_string()
        ]
    );
    assert_eq!(arrow_f64(&body, "temp"), vec![1.5, 2.5, 3.5]);
    assert_eq!(arrow_i64(&body, "pressure"), vec![100, 200, 300]);
}

#[tokio::test]
async fn post_arrow_empty_body_returns_all_children() {
    let app = app_for_root(root_with(vec![("weather", weather())]));
    // Absent/empty body means "all children".
    let (status, body) = post(
        &app,
        "/api/v1/container/full/weather?format=arrow",
        json!([]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(arrow_columns(&body).len(), 3);
}

// ---------------------------------------------------------------------------
// error cases (parity ceiling)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn arrow_on_non_xarray_container_is_406() {
    // A plain container has no arrow serializer upstream → 406, exactly as the
    // family-keyed negotiate answers for any other unsupported container format.
    let plain = container(
        vec![("a", f64_var(&[1.0, 2.0], "xarray_data_var", json!({})))],
        vec![], // no xarray_dataset spec
    );
    let app = app_for_root(root_with(vec![("plain", plain)]));
    let (status, _) = get(&app, "/api/v1/container/full/plain?format=arrow").await;
    assert_eq!(status, StatusCode::NOT_ACCEPTABLE);
}

// F3: a float16 data var is served by csv/parquet (the server-side table
// serializer path) but NOT by the arrow-wire export — the Rust client's
// wide-table decoder handles only the six primitives, so float16 stays a 406 on
// arrow, exactly as before.
#[tokio::test]
async fn arrow_wire_float16_data_var_is_406() {
    let app = app_for_root(root_with(vec![("weather", weather_f16())]));
    let (status, _) = get(&app, "/api/v1/container/full/weather?format=arrow").await;
    assert_eq!(status, StatusCode::NOT_ACCEPTABLE);
}

#[tokio::test]
async fn arrow_child_without_coord_or_data_var_spec_is_422() {
    // Parity with `as_dataset`: every variable must be a coord or data var.
    let malformed = container(
        vec![
            ("time", f64_var(&[1.0, 2.0], "xarray_coord", json!({}))),
            ("bad", f64_var(&[3.0, 4.0], "some_other_spec", json!({}))),
        ],
        vec![Spec::new("xarray_dataset")],
    );
    let app = app_for_root(root_with(vec![("ds", malformed)]));
    let (status, _) = get(&app, "/api/v1/container/full/ds?format=arrow").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

// ---------------------------------------------------------------------------
// F1 — listed-order Accept negotiation. A wide-table media type (or its csv
// alias) must not pre-empt an EARLIER-listed container-servable type: the FIRST
// serviceable type in the Accept list wins (upstream core.py:396-425).
// ---------------------------------------------------------------------------

/// GET returning `(status, content-type, body)` for an explicit Accept header.
async fn get_accept_ct(
    app: &axum::Router,
    uri: &str,
    accept: &str,
) -> (StatusCode, String, Vec<u8>) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(uri)
                .header("accept", accept)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, ct, bytes.to_vec())
}

/// Regression: `Accept: text/html, text/plain` on a NON-xarray container was
/// 200 HTML before the csv-alias commit; scanning the whole Accept for a
/// wide-table type then committed to the csv export (text/plain is a csv alias)
/// and 406'd the plain container. Listed-order negotiation serves the
/// earlier-listed, container-servable text/html.
#[tokio::test]
async fn accept_html_before_plain_alias_serves_html_on_plain_container() {
    let plain = container(
        vec![("a", f64_var(&[1.0, 2.0], "xarray_data_var", json!({})))],
        vec![], // not an xarray_dataset
    );
    let app = app_for_root(root_with(vec![("plain", plain)]));
    let (status, ct, _body) =
        get_accept_ct(&app, "/api/v1/container/full/plain", "text/html, text/plain").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        ct.starts_with("text/html"),
        "content-type should be text/html, got {ct:?}"
    );
}

/// On an xarray_dataset the same header must serve the earlier-listed HTML, not
/// CSV — the csv alias (text/plain) is listed AFTER text/html and must not win.
#[tokio::test]
async fn accept_html_before_plain_alias_serves_html_on_xarray_dataset() {
    let app = app_for_root(root_with(vec![("weather", weather())]));
    let (status, ct, body) =
        get_accept_ct(&app, "/api/v1/container/full/weather", "text/html, text/plain").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        ct.starts_with("text/html"),
        "content-type should be text/html, got {ct:?}"
    );
    assert!(
        !String::from_utf8_lossy(&body).starts_with("time,temp,pressure"),
        "must NOT serve the csv wide table when html is listed first"
    );
}

/// When the csv alias is listed FIRST it wins: the container family cannot serve
/// csv, the xarray_dataset spec can, so the first serviceable type in order is
/// the wide-table csv.
#[cfg(feature = "csv")]
#[tokio::test]
async fn accept_csv_before_html_serves_csv_on_xarray_dataset() {
    let app = app_for_root(root_with(vec![("weather", weather())]));
    let (status, ct, body) =
        get_accept_ct(&app, "/api/v1/container/full/weather", "text/csv, text/html").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        ct.starts_with("text/csv"),
        "content-type should be text/csv, got {ct:?}"
    );
    assert!(String::from_utf8_lossy(&body).starts_with("time,temp,pressure"));
}

// The non-arrow container listing is unaffected by the arrow interception.
#[tokio::test]
async fn non_arrow_container_full_still_lists_children() {
    let app = app_for_root(root_with(vec![("weather", weather())]));
    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/container/full/weather?format=application/json")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    // `application/json` is the `{contents, metadata}` tree, one entry per child.
    // This is the P8 boundary: `application/json` is NOT intercepted for the
    // wide-table export (the Container family owns it), so it still lists children.
    assert_eq!(
        v["contents"].as_object().map(|o| o.len()),
        Some(3),
        "3 child entries in the container listing"
    );
}

// ---------------------------------------------------------------------------
// csv / parquet wide-table export (additive `xarray_dataset` formats)
// ---------------------------------------------------------------------------

/// GET with an explicit `Accept` header (empty string sends none), so the
/// `?format=` vs `Accept` resolution can be exercised for non-arrow formats.
#[cfg(any(feature = "csv", feature = "parquet"))]
async fn get_accept(app: &axum::Router, uri: &str, accept: &str) -> (StatusCode, Vec<u8>) {
    let mut builder = Request::builder().method(Method::GET).uri(uri);
    if !accept.is_empty() {
        builder = builder.header("accept", accept);
    }
    let resp = app
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

#[cfg(feature = "csv")]
mod csv_export {
    use super::*;

    /// `?format=csv` flattens the dataset to one CSV table (upstream
    /// `serialize_dataset_csv`, xarray.py:80 → `to_dataframe()` → table
    /// `serialize_csv`). f64 renders as `N.0`, i64 bare, header + one row per index.
    #[tokio::test]
    async fn get_csv_flattens_wide_table() {
        let app = app_for_root(root_with(vec![("weather", weather())]));
        let (status, body) =
            get_accept(&app, "/api/v1/container/full/weather?format=csv", "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            String::from_utf8(body).unwrap(),
            "time,temp,pressure\n10.0,1.5,100\n20.0,2.5,200\n30.0,3.5,300\n",
            "csv is the flattened wide table"
        );
    }

    /// The `field=` projection restricts and orders the CSV columns.
    #[tokio::test]
    async fn get_csv_field_projection() {
        let app = app_for_root(root_with(vec![("weather", weather())]));
        let (status, body) = get_accept(
            &app,
            "/api/v1/container/full/weather?format=csv&field=time&field=pressure",
            "",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            String::from_utf8(body).unwrap(),
            "time,pressure\n10.0,100\n20.0,200\n30.0,300\n",
            "only the projected columns, in requested order"
        );
    }

    /// With no `?format=`, an `Accept: text/csv` header drives the interception.
    #[tokio::test]
    async fn csv_via_accept_header() {
        let app = app_for_root(root_with(vec![("weather", weather())]));
        let (status, body) = get_accept(&app, "/api/v1/container/full/weather", "text/csv").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            String::from_utf8(body)
                .unwrap()
                .starts_with("time,temp,pressure\n"),
            "Accept: text/csv serves the flattened wide table"
        );
    }

    /// F2: `?format=text/plain` is one of the three csv aliases upstream
    /// registers for the dataset (serialization/xarray.py:80-81); it must serve
    /// the csv wide table, not fall through to container negotiation → 406.
    #[tokio::test]
    async fn format_text_plain_serves_csv_on_xarray_dataset() {
        let app = app_for_root(root_with(vec![("weather", weather())]));
        let (status, body) = get_accept(
            &app,
            "/api/v1/container/full/weather?format=text/plain",
            "",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            String::from_utf8(body)
                .unwrap()
                .starts_with("time,temp,pressure"),
            "?format=text/plain serves the flattened wide table"
        );
    }

    /// F2: `?format=text/comma-separated-values` (the second csv alias) likewise
    /// serves the csv wide table.
    #[tokio::test]
    async fn format_comma_separated_values_serves_csv_on_xarray_dataset() {
        let app = app_for_root(root_with(vec![("weather", weather())]));
        let (status, body) = get_accept(
            &app,
            "/api/v1/container/full/weather?format=text/comma-separated-values",
            "",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            String::from_utf8(body)
                .unwrap()
                .starts_with("time,temp,pressure"),
            "?format=text/comma-separated-values serves the flattened wide table"
        );
    }

    /// F3: a float16 data var flows end-to-end through the csv ColPlan — the
    /// server widens f16→f32 at column build (arrow's csv writer has no native
    /// Float16 path), and the widened column prints its decimal value, matching
    /// upstream `to_dataframe().to_csv()`.
    #[tokio::test]
    async fn get_csv_float16_data_var() {
        let app = app_for_root(root_with(vec![("weather", weather_f16())]));
        let (status, body) =
            get_accept(&app, "/api/v1/container/full/weather?format=csv", "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            String::from_utf8(body).unwrap(),
            "time,half\n10.0,0.5\n20.0,1.5\n30.0,2.5\n",
            "float16 data var widens to f32 and prints its decimal value"
        );
    }

    /// A plain container has no csv serializer (neither spec nor family) → 406,
    /// exactly as negotiation answers, and matching the arrow gate.
    #[tokio::test]
    async fn csv_on_non_xarray_container_is_406() {
        let plain = container(
            vec![("a", f64_var(&[1.0, 2.0], "xarray_data_var", json!({})))],
            vec![], // no xarray_dataset spec
        );
        let app = app_for_root(root_with(vec![("plain", plain)]));
        let (status, _) = get_accept(&app, "/api/v1/container/full/plain?format=csv", "").await;
        assert_eq!(status, StatusCode::NOT_ACCEPTABLE);
    }

    /// The POST field-list body (the client's >2000-char fallback) also reaches the
    /// csv interception; columns follow the requested order (temp before time).
    #[tokio::test]
    async fn post_csv_field_list_body() {
        let app = app_for_root(root_with(vec![("weather", weather())]));
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/container/full/weather?format=csv")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!(["temp", "time"])).unwrap(),
            ))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8(bytes.to_vec()).unwrap(),
            "temp,time\n1.5,10.0\n2.5,20.0\n3.5,30.0\n",
            "POST body projects columns in requested order"
        );
    }
}

#[cfg(feature = "parquet")]
mod parquet_export {
    use super::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn parquet_batches(bytes: &[u8]) -> Vec<arrow::record_batch::RecordBatch> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
            .unwrap()
            .build()
            .unwrap();
        reader.map(|b| b.unwrap()).collect()
    }

    fn column_names(batch: &arrow::record_batch::RecordBatch) -> Vec<String> {
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// `?format=parquet` encodes the same wide table as parquet (upstream
    /// `serialize_dataset_parquet`, xarray.py:73); it round-trips back to the
    /// original columns/values through the parquet reader.
    #[tokio::test]
    async fn get_parquet_flattens_wide_table() {
        let app = app_for_root(root_with(vec![("weather", weather())]));
        let (status, body) =
            get_accept(&app, "/api/v1/container/full/weather?format=parquet", "").await;
        assert_eq!(status, StatusCode::OK);
        let batches = parquet_batches(&body);
        assert_eq!(batches.len(), 1);
        assert_eq!(column_names(&batches[0]), vec!["time", "temp", "pressure"]);
        let time = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(time.values(), &[10.0, 20.0, 30.0]);
        let temp = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(temp.values(), &[1.5, 2.5, 3.5]);
        let pressure = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(pressure.values(), &[100, 200, 300]);
    }

    /// F3: a float16 data var round-trips through the parquet writer as a
    /// widened Float32 column (parquet has no native Float16 either).
    #[tokio::test]
    async fn get_parquet_float16_data_var() {
        let app = app_for_root(root_with(vec![("weather", weather_f16())]));
        let (status, body) =
            get_accept(&app, "/api/v1/container/full/weather?format=parquet", "").await;
        assert_eq!(status, StatusCode::OK);
        let batches = parquet_batches(&body);
        assert_eq!(column_names(&batches[0]), vec!["time", "half"]);
        let half = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .expect("float16 data var widened to Float32");
        assert_eq!(half.values(), &[0.5, 1.5, 2.5]);
    }

    /// `Accept: application/x-parquet` (no `?format=`) drives the interception.
    #[tokio::test]
    async fn parquet_via_accept_header() {
        let app = app_for_root(root_with(vec![("weather", weather())]));
        let (status, body) = get_accept(
            &app,
            "/api/v1/container/full/weather",
            "application/x-parquet",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let batches = parquet_batches(&body);
        assert_eq!(column_names(&batches[0]), vec!["time", "temp", "pressure"]);
    }

    /// A plain container has no parquet serializer → 406.
    #[tokio::test]
    async fn parquet_on_non_xarray_container_is_406() {
        let plain = container(
            vec![("a", f64_var(&[1.0, 2.0], "xarray_data_var", json!({})))],
            vec![],
        );
        let app = app_for_root(root_with(vec![("plain", plain)]));
        let (status, _) = get_accept(&app, "/api/v1/container/full/plain?format=parquet", "").await;
        assert_eq!(status, StatusCode::NOT_ACCEPTABLE);
    }
}
