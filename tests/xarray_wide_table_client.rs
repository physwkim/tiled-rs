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

use async_trait::async_trait;
use bytes::Bytes;
use indexmap::IndexMap;
use serde_json::json;
use tokio::net::TcpListener;

use tiled_rs::access::{AccessPolicy, Decision, NodeContext};
use tiled_rs::adapters::{ArrayAdapter, MapAdapter};
use tiled_rs::auth::{Principal, ScopeSet};
use tiled_rs::client::{DatasetClient, from_uri};
use tiled_rs::core::adapters::{AnyAdapter, BaseAdapter, BoxFuture, ContainerAdapter};
use tiled_rs::core::dtype::{BuiltinDType, Endianness, Kind};
use tiled_rs::core::error::Result as AdapterResult;
use tiled_rs::core::queries::{AccessBlobFilter, Query};
use tiled_rs::core::structures::{ContainerStructure, Spec, StructureFamily};

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
    app_for_root_with_policy(root, None)
}

/// Like [`app_for_root`] but installs `access_policy`. `api_key`/`auth_db` stay
/// unset so the server is in the `no_auth_configured` dev mode (anonymous full
/// scope); the policy's `list_filter` still runs, so the wide-table export
/// routes child enumeration through the access filter.
fn app_for_root_with_policy(
    root: Arc<dyn ContainerAdapter>,
    access_policy: Option<Arc<dyn AccessPolicy>>,
) -> axum::Router {
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree: root,
        serialization_registry: registry,
        query_names: tiled_rs::core::queries::Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: None,
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
        access_policy,
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

async fn spawn(root: Arc<dyn ContainerAdapter>) -> String {
    spawn_app(app_for_root(root)).await
}

async fn spawn_with_policy(
    root: Arc<dyn ContainerAdapter>,
    policy: Arc<dyn AccessPolicy>,
) -> String {
    spawn_app(app_for_root_with_policy(root, Some(policy))).await
}

async fn spawn_app(app: axum::Router) -> String {
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

// ---------------------------------------------------------------------------
// Access-filter enforcement (Finding 2): the wide-table export must enumerate
// children through the access filter (`search([AccessBlobFilter])`), never raw
// `keys()`. A variable the caller cannot see is absent from every full-export
// format, and a direct `?field=` fetch of it is rejected identically to a
// genuinely-absent field (400 `No such field {key}.`), so its presence never
// leaks.
// ---------------------------------------------------------------------------

/// A container wrapping a `MapAdapter` that HIDES one child from `search` (the
/// access-filtered listing path) while keeping it in `keys`/`get` (the raw
/// path). The in-memory `MapAdapter` cannot tag individual nodes, so this
/// focused stand-in models a backend whose access filter excludes one tagged
/// child: if `serve_xarray_wide_table` enumerated via raw `keys()` the child
/// would leak into the export; routing through `search` hides it.
struct SecretHidingDataset {
    inner: MapAdapter,
    secret: String,
}

impl BaseAdapter for SecretHidingDataset {
    fn structure_family(&self) -> StructureFamily {
        self.inner.structure_family()
    }
    fn metadata(&self) -> &serde_json::Value {
        self.inner.metadata()
    }
    fn specs(&self) -> &[Spec] {
        self.inner.specs()
    }
}

impl ContainerAdapter for SecretHidingDataset {
    fn structure(&self) -> BoxFuture<'_, AdapterResult<ContainerStructure>> {
        self.inner.structure()
    }
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, AdapterResult<Option<AnyAdapter>>> {
        self.inner.get(key)
    }
    fn keys(&self) -> BoxFuture<'_, AdapterResult<Vec<String>>> {
        self.inner.keys()
    }
    fn len(&self) -> BoxFuture<'_, AdapterResult<usize>> {
        self.inner.len()
    }
    fn search<'a>(&'a self, queries: &'a [Query]) -> BoxFuture<'a, AdapterResult<Vec<String>>> {
        Box::pin(async move {
            let mut keys = self.inner.keys().await?;
            if queries
                .iter()
                .any(|q| matches!(q, Query::AccessBlobFilter(_)))
            {
                keys.retain(|k| k != &self.secret);
            }
            Ok(keys)
        })
    }
}

/// An `xarray_dataset` with visible coord `time` + data vars `temp`/`pressure`
/// and a HIDDEN data var `secret_zzz`. The wrapper drops `secret_zzz` from the
/// access-filtered listing.
fn secret_weather() -> AnyAdapter {
    let mut m = IndexMap::new();
    m.insert(
        "time".to_string(),
        f64_var(&[10.0, 20.0, 30.0], "xarray_coord", json!({})),
    );
    m.insert(
        "temp".to_string(),
        f64_var(&[1.5, 2.5, 3.5], "xarray_data_var", json!({})),
    );
    m.insert(
        "pressure".to_string(),
        i64_var(&[100, 200, 300], "xarray_data_var"),
    );
    m.insert(
        "secret_zzz".to_string(),
        f64_var(&[9.0, 9.0, 9.0], "xarray_data_var", json!({})),
    );
    let inner = MapAdapter::new(m, json!({}), vec![Spec::new("xarray_dataset")]);
    AnyAdapter::Container(Arc::new(SecretHidingDataset {
        inner,
        secret: "secret_zzz".to_string(),
    }))
}

/// Access policy that grants full scope and returns a non-`None` list filter, so
/// `serve_xarray_wide_table` routes child enumeration through `search`.
struct ListFilterPolicy;

#[async_trait]
impl AccessPolicy for ListFilterPolicy {
    async fn anonymous_decision(&self, _ctx: NodeContext<'_>) -> Decision {
        Decision {
            scopes: ScopeSet::full(),
        }
    }

    async fn principal_decision(
        &self,
        _principal: &Principal,
        session_scopes: &ScopeSet,
        _authn_access_tags: Option<&[String]>,
        _ctx: NodeContext<'_>,
    ) -> Decision {
        Decision {
            scopes: session_scopes.clone(),
        }
    }

    async fn list_filter(
        &self,
        _principal: Option<&Principal>,
        _session_scopes: &ScopeSet,
        _requested_scopes: &ScopeSet,
        _authn_access_tags: Option<&[String]>,
    ) -> Option<AccessBlobFilter> {
        Some(AccessBlobFilter {
            user_id: None,
            tags: vec!["public".to_string()],
            include_untagged: true,
        })
    }
}

/// The hidden variable is absent from the full export in EVERY wide-table
/// format (csv/arrow/parquet, all derived from the same access-filtered column
/// set), and a visible variable is present. Field names are stored as UTF-8 in
/// all three formats (CSV header, Arrow IPC schema, Parquet footer), so a byte
/// search over the response body is a faithful presence check.
#[tokio::test]
async fn wide_table_hides_access_filtered_variable_in_full_export() {
    let policy: Arc<dyn AccessPolicy> = Arc::new(ListFilterPolicy);
    let base = spawn_with_policy(root_with(vec![("ds", secret_weather())]), policy).await;
    let client = reqwest::Client::new();

    for format in ["csv", "arrow", "parquet"] {
        let resp = client
            .get(format!("{base}/api/v1/container/full/ds?format={format}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "format {format} exports with 200");
        let bytes = resp.bytes().await.unwrap();
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            !text.contains("secret_zzz"),
            "hidden variable must be absent from the {format} export"
        );
        assert!(
            text.contains("temp"),
            "visible variable must be present in the {format} export"
        );
    }
}

/// A direct `?field=` fetch of the hidden variable never leaks its data, and its
/// rejection is INDISTINGUISHABLE from that of a genuinely-absent variable:
/// `?field=secret_zzz` on `ds` (where `secret_zzz` is access-hidden) is
/// byte-identical to the same request on `ds2` (where `secret_zzz` does not
/// exist), so the response is not a presence oracle. Both are 400 `No such field
/// secret_zzz.` (upstream parity for a missing field), NOT a distinguishable 404.
#[tokio::test]
async fn wide_table_field_fetch_of_hidden_variable_matches_absent() {
    let policy: Arc<dyn AccessPolicy> = Arc::new(ListFilterPolicy);
    // `ds` hides `secret_zzz`; sibling `ds2` (plain weather) has no such variable.
    let base = spawn_with_policy(
        root_with(vec![("ds", secret_weather()), ("ds2", weather())]),
        policy,
    )
    .await;
    let client = reqwest::Client::new();

    let hidden = client
        .get(format!(
            "{base}/api/v1/container/full/ds?format=csv&field=secret_zzz"
        ))
        .send()
        .await
        .unwrap();
    let absent = client
        .get(format!(
            "{base}/api/v1/container/full/ds2?format=csv&field=secret_zzz"
        ))
        .send()
        .await
        .unwrap();
    let hidden_status = hidden.status();
    let absent_status = absent.status();
    let hidden_body = hidden.bytes().await.unwrap();
    let absent_body = absent.bytes().await.unwrap();
    assert_eq!(
        hidden_status, 400,
        "a hidden variable requested by ?field= must 400, matching an absent one"
    );
    assert_eq!(
        (hidden_status, &hidden_body),
        (absent_status, &absent_body),
        "access-hidden secret_zzz must be byte-identical to absent secret_zzz"
    );

    // A visible field still projects normally.
    let resp = client
        .get(format!(
            "{base}/api/v1/container/full/ds?format=csv&field=temp"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "a visible ?field= projects normally");
    let text = String::from_utf8_lossy(&resp.bytes().await.unwrap()).into_owned();
    assert!(text.contains("temp") && !text.contains("secret_zzz"));
}

// ---------------------------------------------------------------------------
// dtype cap (Finding 5): the arrow-wire export is capped to the six primitives
// the Rust client's wide-table decoder handles, but csv/parquet run through a
// server-side table serializer that reads any Arrow dtype — so they serve the
// full set upstream `to_dataframe` serves (int8/16, uint8/16, bool).
// ---------------------------------------------------------------------------

/// A 1-D `int16` array child carrying `spec`.
fn i16_var(data: &[i16], spec: &str) -> AnyAdapter {
    let bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
    let dtype = BuiltinDType::new(Endianness::Little, Kind::Integer, 2);
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

/// A 1-D `bool` array child (numpy stores one byte per element) carrying `spec`.
fn bool_var(data: &[bool], spec: &str) -> AnyAdapter {
    let bytes: Vec<u8> = data.iter().map(|&b| b as u8).collect();
    let dtype = BuiltinDType::new(Endianness::NotApplicable, Kind::Boolean, 1);
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

/// An `xarray_dataset` mixing wire-decodable and extended dtypes: coord `idx`
/// (f64), data var `small16` (int16, distinctive values), data var `flag`
/// (bool). All length 3.
fn mixed_dtype_dataset() -> AnyAdapter {
    let mut m = IndexMap::new();
    m.insert(
        "idx".to_string(),
        f64_var(&[0.0, 1.0, 2.0], "xarray_coord", json!({})),
    );
    m.insert(
        "small16".to_string(),
        i16_var(&[258, -259, 260], "xarray_data_var"),
    );
    m.insert(
        "flag".to_string(),
        bool_var(&[true, false, true], "xarray_data_var"),
    );
    AnyAdapter::Container(Arc::new(MapAdapter::new(
        m,
        json!({}),
        vec![Spec::new("xarray_dataset")],
    )))
}

/// int16 and bool variables export via csv and parquet (the server-side table
/// serializer reads any Arrow dtype). The exact int16 values round-trip in the
/// csv body.
#[tokio::test]
async fn wide_table_csv_parquet_serve_extended_dtypes() {
    let base = spawn(root_with(vec![("ds", mixed_dtype_dataset())])).await;
    let client = reqwest::Client::new();

    // csv: exact values are readable text.
    let resp = client
        .get(format!("{base}/api/v1/container/full/ds?format=csv"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "int16/bool export via csv");
    let text = String::from_utf8_lossy(&resp.bytes().await.unwrap()).into_owned();
    for tok in ["small16", "flag", "258", "-259", "260"] {
        assert!(text.contains(tok), "csv body must contain {tok:?}: {text}");
    }

    // parquet: binary, but the column names live as UTF-8 in the footer.
    let resp = client
        .get(format!("{base}/api/v1/container/full/ds?format=parquet"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "int16/bool export via parquet");
    let text = String::from_utf8_lossy(&resp.bytes().await.unwrap()).into_owned();
    assert!(
        text.contains("small16") && text.contains("flag"),
        "parquet footer must name the int16/bool columns"
    );
}

/// Arrow-wire behavior pinned: the same int16/bool dataset is rejected with 406
/// on the arrow export, because the Rust client's wide-table decoder handles
/// only f64/f32/i64/i32/u64/u32. (Keeping the cap is deliberate — widening it
/// would emit bytes the client cannot decode.)
#[tokio::test]
async fn wide_table_arrow_rejects_extended_dtypes() {
    let base = spawn(root_with(vec![("ds", mixed_dtype_dataset())])).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/api/v1/container/full/ds?format=arrow"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        406,
        "arrow-wire export must reject int16/bool (client decoder cap)"
    );
}

/// Finding 6: an unknown `?field=` on the wide table returns 400 "No such field
/// {key}.", matching the sibling projection path and upstream
/// router.py:1444-1449 (not the previous NotFound).
#[tokio::test]
async fn wide_table_unknown_field_is_400() {
    let base = spawn(root_with(vec![("weather", weather())])).await;
    let resp = reqwest::Client::new()
        .get(format!(
            "{base}/api/v1/container/full/weather?format=csv&field=bogus"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "unknown field must be a 400 bad request"
    );
    let text = String::from_utf8_lossy(&resp.bytes().await.unwrap()).into_owned();
    assert!(
        text.contains("No such field bogus."),
        "body must carry the upstream message: {text}"
    );
}

/// An `xarray_dataset` with no variables.
fn empty_dataset() -> AnyAdapter {
    AnyAdapter::Container(Arc::new(MapAdapter::new(
        IndexMap::new(),
        json!({}),
        vec![Spec::new("xarray_dataset")],
    )))
}

/// Finding 8: an empty xarray_dataset builds a zero-row batch instead of a
/// `RecordBatch::try_new` error, so every wide-table format serves an empty 200
/// — matching upstream's empty `to_dataframe` export, not the previous 422.
#[tokio::test]
async fn wide_table_empty_dataset_serves_200() {
    let base = spawn(root_with(vec![("empty", empty_dataset())])).await;
    let client = reqwest::Client::new();
    for format in ["csv", "arrow", "parquet"] {
        let resp = client
            .get(format!(
                "{base}/api/v1/container/full/empty?format={format}"
            ))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let body = String::from_utf8_lossy(&resp.bytes().await.unwrap()).into_owned();
        assert_eq!(
            status, 200,
            "empty dataset must serve an empty 200 for {format}, not a RecordBatch error; body={body}"
        );
    }
}

/// Finding 11: upstream registers the dataset CSV serializer under `text/csv`,
/// `text/comma-separated-values`, and `text/plain` (serialization/xarray.py:80-81).
/// A request whose only `Accept` is one of the two aliases must reach the CSV
/// wide-table export (200, `text/csv` body), not fall through to 406.
#[tokio::test]
async fn wide_table_accept_csv_aliases_serve_csv() {
    let base = spawn(root_with(vec![("weather", weather())])).await;
    let client = reqwest::Client::new();
    for alias in ["text/comma-separated-values", "text/plain"] {
        let resp = client
            .get(format!("{base}/api/v1/container/full/weather"))
            .header("accept", alias)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "Accept: {alias} must serve the CSV wide-table export"
        );
        let ct = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        assert!(
            ct.starts_with("text/csv"),
            "Accept: {alias} must yield a text/csv response, got {ct:?}"
        );
        let text = String::from_utf8_lossy(&resp.bytes().await.unwrap()).into_owned();
        for tok in ["time", "temp", "pressure"] {
            assert!(
                text.contains(tok),
                "csv body for Accept: {alias} must name column {tok:?}: {text}"
            );
        }
    }
}
