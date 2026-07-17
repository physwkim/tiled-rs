//! Reverse-proxy `root_path` sub-path support (Wave-25 P10).
//!
//! When the server is fronted by a proxy under a sub-path (clients see
//! `https://host/instrument1/api/v1/...`), the proxy strips the prefix before
//! the request reaches us — so routes stay mounted at `/api/v1/...` and only the
//! *generated* absolute links must carry the prefix. Mirrors upstream tiled's
//! `get_root_url_low_level` (`tiled/server/utils.py:82-85`), which prepends the
//! ASGI `scope["root_path"]` to every link it builds.
//!
//! Uses `tower::ServiceExt::oneshot` for in-process testing with no TCP bind.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use indexmap::IndexMap;
use tower::ServiceExt;

use tiled_rs::adapters::{ArrayAdapter, MapAdapter};
use tiled_rs::core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_rs::core::queries::Query;
use tiled_rs::server::state::normalize_root_path;

fn build_test_tree() -> MapAdapter {
    let mut mapping = IndexMap::new();
    let data: Vec<f64> = (0..10).map(|i| i as f64).collect();
    let arr = ArrayAdapter::from_f64_1d(&data, serde_json::json!({"element": "Cu"}));
    mapping.insert("some_array".to_string(), AnyAdapter::Array(Arc::new(arr)));
    MapAdapter::new(mapping, serde_json::json!({"description": "root"}), vec![])
}

/// Build an app whose `AppState.root_path` is the already-normalized `root_path`
/// (as the CLI wiring stores it). `base_url` is the optional static override;
/// `None` makes links derive from the request Host header, which is the branch
/// `root_path` augments.
fn build_app(root_path: &str, base_url: Option<String>) -> axum::Router {
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(build_test_tree());
    let registry = Arc::new(tiled_rs::serialization::default_registry());

    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url,
        root_path: root_path.to_string(),
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
        enable_web: true,
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

/// Issue a GET with a fixed Host header (the reverse-proxy case sends the
/// *stripped* path — no prefix — because the proxy already removed it) and
/// return `(status, json_body)`.
async fn get(app: &axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let req = Request::builder()
        .uri(uri)
        .header("host", "host.example:8000")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    (status, body)
}

// --- Boundary: default (no root_path) — links byte-identical to a direct deploy.

#[tokio::test]
async fn default_no_root_path_links_have_no_prefix() {
    let app = build_app("", None);

    // About self link + meta.root_path default.
    let (status, about) = get(&app, "/api/v1/").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        about["links"]["self"].as_str().unwrap(),
        "http://host.example:8000/api/v1/"
    );
    // No proxy => About meta.root_path is the "/api" no-proxy default
    // (upstream `router.py:301`).
    assert_eq!(about["meta"]["root_path"].as_str().unwrap(), "/api");

    // Item self link — no prefix.
    let (status, item) = get(&app, "/api/v1/metadata/some_array").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        item["data"]["links"]["self"].as_str().unwrap(),
        "http://host.example:8000/api/v1/metadata/some_array"
    );

    // Search pagination self — no prefix.
    let (status, search) = get(&app, "/api/v1/search/?page[offset]=0&page[limit]=10").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        search["links"]["self"]
            .as_str()
            .unwrap()
            .starts_with("http://host.example:8000/api/v1/search/"),
        "got: {}",
        search["links"]["self"]
    );
}

// --- Boundary: root_path set — About, search, and item links all carry it.

#[tokio::test]
async fn root_path_set_prefixes_about_search_and_item_links() {
    let app = build_app("/instrument1", None);

    // About: self link and meta.root_path both carry the prefix.
    let (status, about) = get(&app, "/api/v1/").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        about["links"]["self"].as_str().unwrap(),
        "http://host.example:8000/instrument1/api/v1/"
    );
    assert_eq!(about["meta"]["root_path"].as_str().unwrap(), "/instrument1");

    // Item: self + array `full` link carry the prefix. Note the request URI
    // itself is the *stripped* path `/api/v1/metadata/...` (the proxy removed
    // `/instrument1`) yet still routes — proving mounting is unchanged.
    let (status, item) = get(&app, "/api/v1/metadata/some_array").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        item["data"]["links"]["self"].as_str().unwrap(),
        "http://host.example:8000/instrument1/api/v1/metadata/some_array"
    );
    assert_eq!(
        item["data"]["links"]["full"].as_str().unwrap(),
        "http://host.example:8000/instrument1/api/v1/array/full/some_array"
    );

    // Search: pagination self link and each child entry's self link carry it.
    let (status, search) = get(&app, "/api/v1/search/?page[offset]=0&page[limit]=10").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        search["links"]["self"]
            .as_str()
            .unwrap()
            .starts_with("http://host.example:8000/instrument1/api/v1/search/"),
        "search self got: {}",
        search["links"]["self"]
    );
    let child_self = search["data"][0]["links"]["self"].as_str().unwrap();
    assert!(
        child_self.starts_with("http://host.example:8000/instrument1/api/v1/metadata/"),
        "child self got: {child_self}"
    );

    // No malformed double slashes anywhere in the generated links.
    for link in [
        about["links"]["self"].as_str().unwrap(),
        item["data"]["links"]["self"].as_str().unwrap(),
        search["links"]["self"].as_str().unwrap(),
    ] {
        let after_scheme = &link["http://".len()..];
        assert!(
            !after_scheme.contains("//"),
            "malformed double slash in link: {link}"
        );
    }
}

// --- Boundary: trailing-slash normalization — "/instrument1/" ≡ "/instrument1".

#[tokio::test]
async fn trailing_slash_root_path_normalizes_and_prefixes_identically() {
    // The CLI normalizes at the boundary; feed a trailing-slash value through
    // the same helper and confirm it collapses to the canonical prefix.
    let normalized = normalize_root_path("/instrument1/");
    assert_eq!(normalized, "/instrument1");

    let app = build_app(&normalized, None);
    let (status, about) = get(&app, "/api/v1/").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        about["links"]["self"].as_str().unwrap(),
        "http://host.example:8000/instrument1/api/v1/"
    );
}

// --- Boundary: explicit base_url override is the complete base; root_path is
//     NOT layered on top of it (design decision — the override owns the whole
//     URL, any prefix included).

#[tokio::test]
async fn explicit_base_url_override_is_not_further_prefixed() {
    let app = build_app("/instrument1", Some("http://public.example".to_string()));
    let (status, about) = get(&app, "/api/v1/").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        about["links"]["self"].as_str().unwrap(),
        "http://public.example/api/v1/"
    );
    assert!(
        !about["links"]["self"]
            .as_str()
            .unwrap()
            .contains("instrument1"),
        "explicit base_url must not be prefixed with root_path"
    );
}

// --- Metadata endpoint `?root_path=true` param (upstream router.py:463,508).
//     When requested, the response `meta.root_path` carries the mount prefix;
//     unset root_path yields "/" (distinct from the About endpoint's "/api").
//     Absent/false param leaves `meta` unset entirely.

#[tokio::test]
async fn metadata_root_path_param_reports_prefix_when_configured() {
    let app = build_app("/instrument1", None);
    let (status, item) = get(&app, "/api/v1/metadata/some_array?root_path=true").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        item["meta"]["root_path"].as_str().unwrap(),
        "/instrument1",
        "?root_path=true must report the configured mount prefix in meta"
    );
}

#[tokio::test]
async fn metadata_root_path_param_defaults_to_slash_when_unset() {
    // No proxy configured: upstream router.py:508 is `... or "/"`, so the
    // metadata endpoint's unset value is "/" — NOT the About endpoint's "/api".
    let app = build_app("", None);
    let (status, item) = get(&app, "/api/v1/metadata/some_array?root_path=true").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        item["meta"]["root_path"].as_str().unwrap(),
        "/",
        "with no root_path configured, ?root_path=true must report \"/\""
    );
}

#[tokio::test]
async fn metadata_absent_root_path_param_omits_meta() {
    // Absent param → `meta` unset (serialized as absent, not `{}` or null),
    // matching the prior behavior. Verified for both a configured prefix and
    // the direct-deploy case so the param — not the deployment — gates `meta`.
    for root in ["", "/instrument1"] {
        let app = build_app(root, None);
        let (status, item) = get(&app, "/api/v1/metadata/some_array").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            item.get("meta").is_none() || item["meta"].is_null(),
            "no ?root_path param must leave meta absent (root={root:?}), got: {}",
            item["meta"]
        );
    }
}

#[tokio::test]
async fn metadata_root_path_false_omits_meta() {
    // An explicit false value must behave like absence.
    let app = build_app("/instrument1", None);
    let (status, item) = get(&app, "/api/v1/metadata/some_array?root_path=false").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        item.get("meta").is_none() || item["meta"].is_null(),
        "?root_path=false must leave meta absent, got: {}",
        item["meta"]
    );
}
