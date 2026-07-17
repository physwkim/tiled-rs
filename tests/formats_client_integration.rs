//! Client-side `formats` parity: the tiled-rs CLIENT reporting which formats
//! the server can export a node as, the way upstream's `BaseClient.formats`
//! property does (`base.py:503`).
//!
//! Drives the real client stack (`from_uri` → `ContainerClient::get` →
//! `BaseClient::formats`) against a live in-process `tiled-server` on an
//! ephemeral TCP port. The formats come from the server's About payload
//! (`GET /api/v1/`), so the assertions tie the client result to the server's
//! own `serialization_registry.all_formats()` contract rather than a hardcoded
//! list. Covers an array leaf, the root container, and the parity note that a
//! node's specs contribute nothing against a stock Rust server (whose About
//! `formats` map is keyed solely by structure family).

use std::sync::Arc;

use indexmap::IndexMap;
use tokio::net::TcpListener;

use tiled_rs::adapters::{ArrayAdapter, MapAdapter, RaggedAdapter};
use tiled_rs::core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_rs::core::queries::Query;
use tiled_rs::core::structures::Spec;

/// The server registry's exported media types for one structure family, sorted
/// and de-duplicated — exactly the shape `BaseClient::formats` must return for a
/// node of that family with no spec-keyed formats.
fn expected_formats(family: &str) -> Vec<String> {
    let registry = tiled_rs::serialization::default_registry();
    let mut v = registry
        .all_formats()
        .get(family)
        .cloned()
        .unwrap_or_default();
    v.sort();
    v.dedup();
    v
}

/// Root container carrying a spec and two children: a plain array `plain` and a
/// ragged node `ragged`. The root's spec exercises `formats`' per-spec lookup,
/// which is inert against a stock Rust server (its About `formats` map has no
/// spec-name keys); the ragged child covers the family that previously dropped
/// out of the server's About `formats` map.
fn build_root() -> Arc<dyn ContainerAdapter> {
    let data: Vec<f64> = (0..4).map(|i| i as f64).collect();
    let arr = ArrayAdapter::from_f64_1d(&data, serde_json::json!({}));
    let ragged = RaggedAdapter::from_rows_f64(
        vec![vec![1.0, 2.0, 3.0], vec![4.0], vec![5.0, 6.0]],
        serde_json::json!({}),
        vec![],
    );
    let mut mapping = IndexMap::new();
    mapping.insert("plain".to_string(), AnyAdapter::Array(Arc::new(arr)));
    mapping.insert("ragged".to_string(), AnyAdapter::Ragged(Arc::new(ragged)));
    Arc::new(MapAdapter::new(
        mapping,
        serde_json::json!({"description": "formats test catalog"}),
        vec![Spec::new("catalog_only_spec")],
    ))
}

/// Spawn `tiled-server` over `root_tree` on an ephemeral port; return base URL.
async fn spawn_server(root_tree: Arc<dyn ContainerAdapter>) -> String {
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
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
    };
    let app = tiled_rs::server::build_app(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    base_url
}

#[tokio::test]
async fn formats_array_node_matches_server_array_formats() {
    // Client gap #9: an array leaf's formats() is the server's registered array
    // media types (base.py:503, structure-family branch), sorted and deduped.
    let base = spawn_server(build_root()).await;
    let expected = expected_formats("array");
    assert!(
        !expected.is_empty(),
        "the default registry serves at least one array format — test must not be vacuous"
    );

    let root = tiled_rs::client::from_uri(&base)
        .await
        .unwrap()
        .into_container()
        .unwrap();
    let node = root.get("plain").await.unwrap();
    let formats = node
        .base()
        .unwrap()
        .formats()
        .await
        .expect("array node formats");
    assert_eq!(formats, expected, "array formats match the server registry");
    // Sorted, de-duplicated (the property's contract).
    let mut sorted = formats.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(formats, sorted, "formats must be sorted and de-duplicated");
}

#[tokio::test]
async fn formats_container_node_ignores_unknown_specs() {
    // The root carries a spec ("catalog_only_spec") the server's formats map
    // does not key on, so formats() returns exactly the container-family
    // formats — the spec branch contributes nothing (the documented parity note
    // that the Rust About map is keyed solely by structure family).
    let base = spawn_server(build_root()).await;
    let expected = expected_formats("container");

    let root = tiled_rs::client::from_uri(&base).await.unwrap();
    let formats = root
        .base()
        .unwrap()
        .formats()
        .await
        .expect("container node formats");
    assert_eq!(
        formats, expected,
        "container formats match the server registry; the unknown spec adds nothing"
    );
}

#[tokio::test]
async fn formats_ragged_node_is_non_empty() {
    // Regression for the server-side follow-up: a ragged node's formats() is
    // non-empty. Before the `all_formats()` family list included `ragged`, the
    // server's About map had no `ragged` key and this returned `[]`. It must now
    // match the server's registered ragged media types.
    let base = spawn_server(build_root()).await;
    let expected = expected_formats("ragged");
    assert!(
        !expected.is_empty(),
        "the default registry serves at least one ragged format"
    );

    let root = tiled_rs::client::from_uri(&base)
        .await
        .unwrap()
        .into_container()
        .unwrap();
    let node = root.get("ragged").await.unwrap();
    let formats = node
        .base()
        .unwrap()
        .formats()
        .await
        .expect("ragged node formats");
    assert!(
        !formats.is_empty(),
        "a ragged node must report at least one exportable format"
    );
    assert_eq!(
        formats, expected,
        "ragged formats match the server registry (regression: ragged was dropped from the map)"
    );
}
