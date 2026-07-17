//! `DatasetClient` wide-table optimisation, driven end to end against a live
//! in-process server over an ephemeral TCP port.
//!
//! `DatasetClient::read(.., optimize_wide_table = true)` fetches all scalar-ish
//! variables of an `xarray_dataset` in one Arrow IPC call to `/container/full`
//! (`tiled/client/xarray.py::_WideTableFetcher`). Short field lists ride a GET
//! (`?field=…`); once the projected URL would exceed `URL_CHARACTER_LIMIT`
//! (2000, `base.py:128`) the field list moves into a POST JSON body
//! (`xarray.py:206`). Both cases are exercised here against the real server route
//! added alongside this client change.
//!
//! The wide path carries column *values* only, not per-variable `attrs`; a
//! per-array (narrow) fallback would surface them. Asserting `attrs == Null`
//! therefore proves the wide path (not the fallback) actually ran.

#![cfg(feature = "arrow-ipc")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use indexmap::IndexMap;
use serde_json::json;
use tokio::net::TcpListener;

use tiled_rs::adapters::{ArrayAdapter, MapAdapter};
use tiled_rs::client::{DatasetClient, from_uri};
use tiled_rs::core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_rs::core::dtype::{BuiltinDType, Endianness, Kind};
use tiled_rs::core::structures::Spec;

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

/// `weather`: an `xarray_dataset` with a coord `time` (f64), a data var `temp`
/// (f64, carries `attrs`), and a data var `pressure` (i64). All length 3.
fn weather() -> AnyAdapter {
    let mut m = IndexMap::new();
    m.insert(
        "time".to_string(),
        f64_var(&[10.0, 20.0, 30.0], "xarray_coord", json!({})),
    );
    m.insert(
        "temp".to_string(),
        f64_var(
            &[1.5, 2.5, 3.5],
            "xarray_data_var",
            json!({"attrs": {"units": "K"}}),
        ),
    );
    m.insert(
        "pressure".to_string(),
        i64_var(&[100, 200, 300], "xarray_data_var"),
    );
    AnyAdapter::Container(Arc::new(MapAdapter::new(
        m,
        json!({}),
        vec![Spec::new("xarray_dataset")],
    )))
}

/// A wide `xarray_dataset`: one coord + `N` data vars whose combined
/// `&field=<name>` query would exceed `URL_CHARACTER_LIMIT` (2000), forcing the
/// client onto the POST fallback.
fn wide_dataset(n: usize) -> AnyAdapter {
    let mut m = IndexMap::new();
    m.insert(
        "index".to_string(),
        f64_var(&[0.0, 1.0, 2.0], "xarray_coord", json!({})),
    );
    for i in 0..n {
        let name = format!("channel_measurement_{i:04}"); // 24 chars
        let vals = [i as f64, i as f64 + 0.5, i as f64 + 1.0];
        m.insert(
            name,
            f64_var(&vals, "xarray_data_var", json!({"attrs": {"unit": "v"}})),
        );
    }
    AnyAdapter::Container(Arc::new(MapAdapter::new(
        m,
        json!({}),
        vec![Spec::new("xarray_dataset")],
    )))
}

fn root_with(children: Vec<(&str, AnyAdapter)>) -> Arc<dyn ContainerAdapter> {
    let mut m = IndexMap::new();
    for (k, v) in children {
        m.insert(k.to_string(), v);
    }
    Arc::new(MapAdapter::new(m, json!({}), vec![]))
}

/// `base_url: None` so node links derive from the request Host, letting the
/// client follow them back to the ephemeral address.
fn app_for_root(root: Arc<dyn ContainerAdapter>) -> axum::Router {
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree: root,
        serialization_registry: registry,
        query_names: tiled_rs::core::queries::Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: None,
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
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    };
    tiled_rs::server::build_app(state)
}

async fn spawn(root: Arc<dyn ContainerAdapter>) -> String {
    let app = app_for_root(root);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    base
}

async fn read_dataset(base: &str, key: &str) -> tiled_rs::client::Dataset {
    let root = from_uri(base).await.unwrap().into_container().unwrap();
    let node = root.get(key).await.unwrap();
    let ds_container = node.into_container().unwrap();
    DatasetClient::new(ds_container)
        .read(None, true)
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wide_table_get_path_end_to_end() {
    let base = spawn(root_with(vec![("weather", weather())])).await;
    let ds = read_dataset(&base, "weather").await;

    // Coord + both data vars come back with correct values.
    assert_eq!(
        ds.coords.get("time").and_then(|v| v.as_f64_vec()),
        Some(vec![10.0, 20.0, 30.0])
    );
    assert_eq!(
        ds.data_vars.get("temp").and_then(|v| v.as_f64_vec()),
        Some(vec![1.5, 2.5, 3.5])
    );
    assert_eq!(
        ds.data_vars.get("pressure").and_then(|v| v.as_i64_vec()),
        Some(vec![100, 200, 300])
    );

    // attrs dropped ⇒ the wide (GET) path ran, not a per-array narrow fallback.
    assert_eq!(
        ds.data_vars.get("temp").map(|v| &v.attrs),
        Some(&serde_json::Value::Null),
        "wide-table path taken (narrow fallback would carry attrs)"
    );
}

#[tokio::test]
async fn wide_table_post_fallback_end_to_end() {
    // 80 vars × ("&field=" (7) + 24-char name) = 2480 chars of field params
    // alone — past the 2000-char GET limit regardless of host/port, so the client
    // switches to POST (`fetch_wide_arrow`'s POST branch).
    const N: usize = 80;
    let base = spawn(root_with(vec![("wide", wide_dataset(N))])).await;
    let ds = read_dataset(&base, "wide").await;

    assert_eq!(ds.coords.len(), 1, "index coord");
    assert_eq!(ds.data_vars.len(), N, "all {N} data vars returned via POST");
    assert_eq!(
        ds.coords.get("index").and_then(|v| v.as_f64_vec()),
        Some(vec![0.0, 1.0, 2.0])
    );
    // Spot-check a few channels by value.
    let check: HashMap<usize, Vec<f64>> = [0usize, 7, 79]
        .into_iter()
        .map(|i| (i, vec![i as f64, i as f64 + 0.5, i as f64 + 1.0]))
        .collect();
    for (i, expected) in check {
        let name = format!("channel_measurement_{i:04}");
        assert_eq!(
            ds.data_vars.get(&name).and_then(|v| v.as_f64_vec()),
            Some(expected),
            "value round-trip for {name}"
        );
    }
    // Same probe: attrs dropped ⇒ the wide POST path ran, not the narrow fallback.
    let name = format!("channel_measurement_{:04}", 0);
    assert_eq!(
        ds.data_vars.get(&name).map(|v| &v.attrs),
        Some(&serde_json::Value::Null),
        "wide-table POST path taken (narrow fallback would carry attrs)"
    );
}
