//! End-to-end test: SQLite-backed catalog wired into the HTTP server.
//!
//! Exercises register → metadata round-trip → PATCH → DELETE — confirming
//! the new write endpoints persist and the read endpoints see the result.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use tiled_catalog::{Catalog, adapter::UnresolvedLeaf};
use tiled_core::adapters::ContainerAdapter;
use tiled_core::queries::Query;

async fn build_test_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let resolver: Arc<dyn tiled_catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(
        tiled_catalog::CatalogAdapter::root(catalog.clone(), resolver),
    );
    let registry = Arc::new(tiled_serialization::default_registry());
    let state = tiled_server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: Some("http://localhost:8000".into()),
        cors_policy: tiled_server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: Some(catalog),
        auth_db: None,
        issuer: None,
        authenticators: vec![],
        proxied_header_auth: None,
        external_oidc: None,
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
    };
    (tiled_server::build_app(state), dir)
}

async fn json_request(
    app: &axum::Router,
    method: Method,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
}

async fn empty_request(app: &axum::Router, method: Method, uri: &str) -> (StatusCode, Vec<u8>) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, bytes.to_vec())
}

#[tokio::test]
async fn register_then_read_then_patch_then_delete() {
    let (app, _dir) = build_test_app().await;

    // Register a top-level container "expt".
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        serde_json::json!({
            "key": "expt",
            "structure_family": "container",
            "metadata": {"description": "first run"},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "register root: {body}");
    assert_eq!(body["id"], "expt");

    // Register a nested container "expt/scan_1".
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/expt",
        serde_json::json!({
            "key": "scan_1",
            "structure_family": "container",
            "metadata": {"plan_name": "count"},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "register nested: {body}");
    assert_eq!(body["id"], "scan_1");

    // GET metadata for the top-level container — should round-trip from DB.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/expt",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["id"], "expt");
    assert_eq!(
        body["data"]["attributes"]["metadata"]["description"],
        "first run"
    );

    // PATCH metadata.
    let (status, body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/expt",
        serde_json::json!({"metadata": {"description": "updated"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "patch: {body}");
    assert_eq!(body["metadata"]["description"], "updated");

    // GET again — confirms PATCH stuck.
    let (_, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/expt",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(
        body["data"]["attributes"]["metadata"]["description"],
        "updated"
    );

    // DELETE expt cascades to scan_1.
    let (status, _) = empty_request(&app, Method::DELETE, "/api/v1/metadata/expt").await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = empty_request(&app, Method::GET, "/api/v1/metadata/expt").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = empty_request(&app, Method::GET, "/api/v1/metadata/expt/scan_1").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn duplicate_key_at_same_level_returns_422() {
    let (app, _dir) = build_test_app().await;
    let body = serde_json::json!({
        "key": "dup",
        "structure_family": "container",
        "metadata": {},
        "specs": [],
        "data_sources": [],
    });
    let (status, _) = json_request(&app, Method::POST, "/api/v1/register/", body.clone()).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = json_request(&app, Method::POST, "/api/v1/register/", body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn search_pushes_filters_to_sql() {
    let (app, _dir) = build_test_app().await;

    // Seed three nodes with different metadata.
    for (key, plan, count) in [
        ("a", "count", 3),
        ("b", "scan", 7),
        ("c", "count", 12),
    ] {
        let body = serde_json::json!({
            "key": key,
            "structure_family": "container",
            "metadata": {"plan_name": plan, "num_points": count},
            "specs": [],
            "data_sources": [],
        });
        let (status, _) = json_request(&app, Method::POST, "/api/v1/register/", body).await;
        assert_eq!(status, StatusCode::CREATED);
    }

    // filter[eq][condition][key]=plan_name & filter[eq][condition][value]="count"
    let url = "/api/v1/search/?\
        filter[eq][condition][key]=plan_name&\
        filter[eq][condition][value]=%22count%22";
    let (status, body) = json_request(&app, Method::GET, url, serde_json::Value::Null).await;
    assert_eq!(status, StatusCode::OK, "search: {body}");
    let ids: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["a", "c"]);
    assert_eq!(body["meta"]["count"], 2);

    // Comparison: num_points > 5 → b and c.
    let url = "/api/v1/search/?\
        filter[comparison][condition][operator]=gt&\
        filter[comparison][condition][key]=num_points&\
        filter[comparison][condition][value]=5";
    let (status, body) = json_request(&app, Method::GET, url, serde_json::Value::Null).await;
    assert_eq!(status, StatusCode::OK, "comparison: {body}");
    let ids: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["b", "c"]);
}

#[tokio::test]
async fn delete_root_rejected() {
    let (app, _dir) = build_test_app().await;
    let (status, _) = empty_request(&app, Method::DELETE, "/api/v1/metadata/").await;
    // No `*path` segments → 404 from axum routing (no DELETE on the bare
    // collection prefix).
    assert!(matches!(status, StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED));
}
