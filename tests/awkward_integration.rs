//! End-to-end tests for the awkward array read + write paths.
//!
//! Wires an in-memory `MapAdapter` tree with an `AwkwardAdapter` leaf, then
//! drives the full pipeline over HTTP:
//!
//!   GET  /api/v1/metadata/arr    → check structure (form + length)
//!   POST /api/v1/awkward/buffers/arr → filter + fetch buffers (ZIP)
//!   GET  /api/v1/awkward/full/arr    → fetch all buffers (ZIP)
//!   PUT  /api/v1/awkward/full/arr    → write new buffers
//!   GET  /api/v1/awkward/full/arr    → read back, verify updated

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use bytes::Bytes;
use indexmap::IndexMap;
use tower::ServiceExt;

use tiled_rs::adapters::{AwkwardAdapter, MapAdapter};
use tiled_rs::core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_rs::core::queries::Query;
use tiled_rs::core::structures::AwkwardStructure;

/// Build a minimal Tiled app whose root contains one awkward array node.
fn build_awkward_app() -> axum::Router {
    let form = serde_json::json!({
        "class": "NumpyArray",
        "primitive": "float64",
        "form_key": "node0",
        "inner_shape": [],
        "itemsize": 8
    });
    let structure = AwkwardStructure {
        length: 3,
        form: form.clone(),
    };
    let mut buffers: HashMap<String, Bytes> = HashMap::new();
    buffers.insert(
        "node0-data".to_string(),
        Bytes::from(vec![
            0u8, 0, 0, 0, 0, 0, 0xF0, 0x3F, // 1.0 f64 LE
            0, 0, 0, 0, 0, 0, 0, 0x40, // 2.0 f64 LE
            0, 0, 0, 0, 0, 0, 8, 0x40, // 3.0 f64 LE
        ]),
    );

    let adapter = AwkwardAdapter::new(buffers, structure);
    let mut mapping = IndexMap::new();
    mapping.insert("arr".to_string(), AnyAdapter::Awkward(Arc::new(adapter)));

    let root_tree: Arc<dyn ContainerAdapter> =
        Arc::new(MapAdapter::new(mapping, serde_json::json!({}), vec![]));

    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: Some("http://localhost:8000".into()),
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
        enable_web: true,
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

/// Extract named entries from a ZIP archive bytes.
fn unzip(data: &[u8]) -> HashMap<String, Vec<u8>> {
    let cursor = std::io::Cursor::new(data);
    let mut archive = zip::ZipArchive::new(cursor).expect("valid zip");
    let mut out = HashMap::new();
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).unwrap();
        let name = entry.name().to_string();
        let mut buf = Vec::new();
        entry.read_to_end(&mut buf).unwrap();
        out.insert(name, buf);
    }
    out
}

/// Pack a buffer map into an uncompressed ZIP archive.
fn pack_zip(buffers: &HashMap<String, &[u8]>) -> Vec<u8> {
    use std::io::Write;
    use zip::write::SimpleFileOptions;
    let cursor = std::io::Cursor::new(Vec::<u8>::new());
    let mut zip = zip::ZipWriter::new(cursor);
    let opts = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (name, data) in buffers {
        zip.start_file(name, opts).unwrap();
        zip.write_all(data).unwrap();
    }
    zip.finish().unwrap().into_inner()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn get_bytes(app: &axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
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

async fn post_json(
    app: &axum::Router,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, Vec<u8>) {
    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

async fn put_bytes(app: &axum::Router, uri: &str, body: Vec<u8>, content_type: &str) -> StatusCode {
    let req = Request::builder()
        .method(Method::PUT)
        .uri(uri)
        .header("content-type", content_type)
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    resp.status()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// GET /metadata/arr returns structure_family=awkward, form, length.
#[tokio::test]
async fn metadata_reports_awkward_structure() {
    let app = build_awkward_app();
    let (status, body) = get_bytes(&app, "/api/v1/metadata/arr").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&body)
    );

    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let attrs = &json["data"]["attributes"];
    assert_eq!(attrs["structure_family"], "awkward");
    assert_eq!(attrs["structure"]["length"], 3);
    assert_eq!(attrs["structure"]["form"]["class"], "NumpyArray");
}

/// GET /metadata/arr advertises links.full and links.buffers.
#[tokio::test]
async fn metadata_advertises_awkward_links() {
    let app = build_awkward_app();
    let (status, body) = get_bytes(&app, "/api/v1/metadata/arr").await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let links = &json["data"]["links"];
    assert!(
        links["full"]
            .as_str()
            .map(|s| s.contains("/awkward/full/arr"))
            .unwrap_or(false),
        "links.full must point to /awkward/full/arr, got: {links}"
    );
    assert!(
        links["buffers"]
            .as_str()
            .map(|s| s.contains("/awkward/buffers/arr"))
            .unwrap_or(false),
        "links.buffers must point to /awkward/buffers/arr, got: {links}"
    );
}

/// GET /awkward/full/arr returns a ZIP containing the buffer.
#[tokio::test]
async fn awkward_full_get_returns_zip() {
    let app = build_awkward_app();
    let (status, body) = get_bytes(&app, "/api/v1/awkward/full/arr").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&body)
    );

    let entries = unzip(&body);
    assert!(
        entries.contains_key("node0-data"),
        "ZIP must contain node0-data, got keys: {:?}",
        entries.keys().collect::<Vec<_>>()
    );
    // 3 float64 values = 24 bytes
    assert_eq!(entries["node0-data"].len(), 24);
}

/// POST /awkward/buffers/arr with an empty key list returns all buffers.
#[tokio::test]
async fn post_awkward_buffers_empty_keys_returns_all() {
    let app = build_awkward_app();
    let (status, body) =
        post_json(&app, "/api/v1/awkward/buffers/arr", serde_json::json!([])).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&body)
    );

    let entries = unzip(&body);
    assert!(entries.contains_key("node0-data"));
}

/// POST /awkward/buffers/arr with matching form_key returns filtered buffers.
#[tokio::test]
async fn post_awkward_buffers_filters_by_prefix() {
    let app = build_awkward_app();
    let (status, body) = post_json(
        &app,
        "/api/v1/awkward/buffers/arr",
        serde_json::json!(["node0"]),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&body)
    );

    let entries = unzip(&body);
    assert!(
        entries.contains_key("node0-data"),
        "node0-data must be included when form_key=node0"
    );
}

/// PUT /awkward/full/arr + GET round-trip: write new buffers and read them back.
#[tokio::test]
async fn put_then_get_awkward_full_roundtrip() {
    let app = build_awkward_app();

    // Write new buffer: 2 float64 values = 16 bytes
    let new_data: Vec<u8> = vec![
        0u8, 0, 0, 0, 0, 0, 0x10, 0x40, // 4.0 f64 LE
        0, 0, 0, 0, 0, 0, 0x14, 0x40, // 5.0 f64 LE
    ];
    let mut bufs: HashMap<String, &[u8]> = HashMap::new();
    bufs.insert("node0-data".to_string(), &new_data);
    let zip_body = pack_zip(&bufs);

    let put_status = put_bytes(
        &app,
        "/api/v1/awkward/full/arr",
        zip_body,
        "application/zip",
    )
    .await;
    assert_eq!(put_status, StatusCode::OK, "PUT must succeed");

    // Read back and verify the new buffer contents
    let (get_status, body) = get_bytes(&app, "/api/v1/awkward/full/arr").await;
    assert_eq!(get_status, StatusCode::OK);
    let entries = unzip(&body);
    assert_eq!(
        entries["node0-data"], new_data,
        "read-back must match the written buffer"
    );
}

/// GET /awkward/full/arr with Accept: application/zip explicitly works.
#[tokio::test]
async fn awkward_full_accept_zip_header() {
    let app = build_awkward_app();
    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/awkward/full/arr")
        .header("accept", "application/zip")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// Requesting an unsupported format returns 406.
#[tokio::test]
async fn awkward_full_unsupported_format_returns_406() {
    let app = build_awkward_app();
    let (status, _) = get_bytes(&app, "/api/v1/awkward/full/arr?format=text/csv").await;
    assert_eq!(
        status,
        StatusCode::NOT_ACCEPTABLE,
        "application/json and text/csv are unsupported for awkward — expect 406"
    );
}
