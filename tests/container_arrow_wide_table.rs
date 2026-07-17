//! Wide-table Arrow export for `xarray_dataset` containers — the server
//! `/container/full` `?format=arrow` route and its POST field-list fallback.
//!
//! Upstream serves a Container as Arrow via a *spec*-keyed serializer
//! (`serialization/xarray.py:68`, registered under the `xarray_dataset` spec);
//! the Rust serialization registry keys on `StructureFamily` only, so the route
//! mirrors that serializer's logic inline, gated on the spec. `field=` (repeated
//! GET query, upstream `router.py:1352`) / a JSON-array POST body (upstream
//! `router.py:1396`) select the column projection.
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
        streaming_bus: tiled_rs::server::streaming::StreamingBus::new(),
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
    assert_eq!(
        v["contents"].as_object().map(|o| o.len()),
        Some(3),
        "3 child entries in the container listing"
    );
}
