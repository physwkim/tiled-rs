//! End-to-end test: SQLite-backed catalog wired into the HTTP server.
//!
//! Exercises register → metadata round-trip → PATCH → DELETE — confirming
//! the new write endpoints persist and the read endpoints see the result.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use tiled_rs::catalog::{Catalog, adapter::UnresolvedLeaf};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

async fn build_test_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
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
        catalog: Some(catalog),
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
    (tiled_rs::server::build_app(state), dir)
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
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
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
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
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

    // PATCH metadata. The canonical client always sends the HTTP header
    // `Content-Type: application/json` and carries the real patch type in the
    // body; merge-patch here updates `description`.
    let (status, body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/expt",
        serde_json::json!({
            "content-type": "application/merge-patch+json",
            "metadata": {"description": "updated"},
        }),
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

    // Upstream tiled #503: DELETE on a non-empty container is now
    // rejected — explicit rmdir-style emptying is required. F-R: this is a
    // 409 Conflict, matching Python's Conflicts handler (app.py:350-353).
    let (status, _) = empty_request(&app, Method::DELETE, "/api/v1/metadata/expt").await;
    assert_eq!(status, StatusCode::CONFLICT);

    // Empty the child first, THEN the container.
    let (status, _) = empty_request(&app, Method::DELETE, "/api/v1/metadata/expt/scan_1").await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = empty_request(&app, Method::DELETE, "/api/v1/metadata/expt").await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = empty_request(&app, Method::GET, "/api/v1/metadata/expt").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = empty_request(&app, Method::GET, "/api/v1/metadata/expt/scan_1").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// Server H3: `POST /api/v1/metadata/{path}` is the client's common
/// (asset-free) creation endpoint (`tiled/client/container.py:733-740`). It
/// must (a) create + persist like `/register/`, and (b) REJECT data sources
/// carrying externally-managed assets with 400, directing them to `/register/`
/// (Python parity: `router.py:1769`, asset guard at `1794-1799`). Previously
/// the route was absent → Axum answered 405.
#[tokio::test]
async fn post_metadata_creates_asset_free_and_rejects_external_assets() {
    let (app, _dir) = build_test_app().await;

    // (a) Root POST /metadata/ creates a container and persists it.
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "viameta",
            "structure_family": "container",
            "metadata": {"description": "made via /metadata"},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "post /metadata/ root: {body}");
    assert_eq!(body["id"], "viameta");

    // It round-trips from the DB via GET.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/viameta",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["id"], "viameta");
    assert_eq!(
        body["data"]["attributes"]["metadata"]["description"],
        "made via /metadata"
    );

    // Nested POST /metadata/{path} also creates a child under "viameta".
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/viameta",
        serde_json::json!({
            "key": "child",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "post /metadata/viameta: {body}"
    );
    assert_eq!(body["id"], "child");

    // (b) A data source carrying an externally-managed asset is rejected with
    // 400 (must use /register/ instead) and is NOT persisted.
    let (status, _body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "withasset",
            "structure_family": "array",
            "metadata": {},
            "specs": [],
            "data_sources": [
                {
                    "structure_family": "array",
                    "management": "external",
                    "assets": [
                        {"data_uri": "file:///tmp/x.h5", "is_directory": false}
                    ]
                }
            ],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "asset-bearing /metadata create must be rejected"
    );
    let (status, _) = empty_request(&app, Method::GET, "/api/v1/metadata/withasset").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "rejected node must not exist"
    );

    // Cleanup: child first (non-empty container DELETE is 409), then root.
    let (status, _) = empty_request(&app, Method::DELETE, "/api/v1/metadata/viameta/child").await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = empty_request(&app, Method::DELETE, "/api/v1/metadata/viameta").await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

/// Server H2: `GET /api/v1/distinct/{path}` returns the unique metadata-key
/// values, structure families, and specs among a container's children, with
/// optional counts — unblocking the Python client's `distinct()` (404 today).
/// Mirrors Python `get_distinct` (catalog/adapter.py:647-698): each facet is a
/// `GROUP BY` with `COUNT(col)`, so the missing-key group reports count 0, and
/// without `?counts=true` the count is null. Response is the bare
/// `GetDistinctResponse` object; unrequested facets are null.
#[tokio::test]
async fn distinct_groups_metadata_structure_families_and_specs() {
    let (app, _dir) = build_test_app().await;

    let register =
        |key: &str, family: &str, metadata: serde_json::Value, specs: serde_json::Value| {
            serde_json::json!({
                "key": key,
                "structure_family": family,
                "metadata": metadata,
                "specs": specs,
                "data_sources": [],
            })
        };
    for body in [
        register(
            "a",
            "container",
            serde_json::json!({"plan": "count", "n": 1}),
            serde_json::json!([{"name": "xas"}]),
        ),
        register(
            "b",
            "container",
            serde_json::json!({"plan": "count", "n": 2}),
            serde_json::json!([{"name": "xas"}]),
        ),
        register(
            "c",
            "array",
            serde_json::json!({"plan": "scan"}),
            serde_json::json!([]),
        ),
        // "d" lacks the "plan" key → the missing-key (null) group, count 0.
        register(
            "d",
            "table",
            serde_json::json!({"other": 5}),
            serde_json::json!([]),
        ),
    ] {
        let (status, b) = json_request(&app, Method::POST, "/api/v1/register/", body).await;
        assert_eq!(status, StatusCode::CREATED, "register: {b}");
    }

    // metadata facet with counts — group root's children by metadata.plan.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/distinct/?metadata=plan&counts=true",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "distinct: {body}");
    let plan = body["metadata"]["plan"]
        .as_array()
        .expect("metadata.plan is an array");
    let got: Vec<(serde_json::Value, i64)> = plan
        .iter()
        .map(|e| (e["value"].clone(), e["count"].as_i64().unwrap()))
        .collect();
    assert!(
        got.contains(&(serde_json::json!("count"), 2)),
        "got: {got:?}"
    );
    assert!(
        got.contains(&(serde_json::json!("scan"), 1)),
        "got: {got:?}"
    );
    assert!(
        got.contains(&(serde_json::json!(null), 0)),
        "missing-key null group with count 0: {got:?}"
    );
    // Unrequested facets are null (bare object, not wrapped in {data,...}).
    assert!(body["structure_families"].is_null());
    assert!(body["specs"].is_null());

    // structure_families facet with counts.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/distinct/?structure_families=true&counts=true",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let mut fams = std::collections::HashMap::new();
    for e in body["structure_families"].as_array().unwrap() {
        fams.insert(
            e["value"].as_str().unwrap().to_string(),
            e["count"].as_i64().unwrap(),
        );
    }
    assert_eq!(fams.get("container"), Some(&2));
    assert_eq!(fams.get("array"), Some(&1));
    assert_eq!(fams.get("table"), Some(&1));

    // specs facet with counts — two distinct specs values ([] and [{name}]).
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/distinct/?specs=true&counts=true",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let specs = body["specs"].as_array().unwrap();
    assert_eq!(specs.len(), 2, "two distinct specs values: {specs:?}");
    let total: i64 = specs.iter().map(|e| e["count"].as_i64().unwrap()).sum();
    assert_eq!(total, 4, "every child counted once: {specs:?}");
    assert!(
        specs.iter().any(|e| e["value"] == serde_json::json!([])),
        "the empty-specs group is present: {specs:?}"
    );

    // Without ?counts, every entry's count is null (Python's counts=False).
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/distinct/?metadata=plan",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["metadata"]["plan"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e["count"].is_null()),
        "no ?counts → count null: {body}"
    );
}

/// F-F: DELETE on a subtree holding an internally-managed (writable) data
/// source is refused by default with 409 (mirrors Python `WouldDeleteData`,
/// app.py:367-374). Passing `?external_only=false` forces the delete.
#[tokio::test]
async fn delete_internally_managed_requires_external_only_false() {
    let (app, dir) = build_test_app().await;
    // Asset path is under the test tempdir and never created on disk, so the
    // forced delete below — which now reclaims managed file:// assets — has no
    // real file to touch (keeps the test hermetic).
    let data_uri =
        tiled_rs::core::file_uri::path_to_file_uri(&dir.path().join("frame.h5")).unwrap();

    // Register an array node carrying a *writable* data source.
    let register = |key: &str| {
        serde_json::json!({
            "key": key,
            "structure_family": "array",
            "metadata": {},
            "specs": [],
            "data_sources": [{
                "structure_family": "array",
                "mimetype": "application/x-hdf5",
                "management": "writable",
                "assets": [{
                    "data_uri": data_uri.clone(),
                    "is_directory": false,
                    "parameter": "data_uri"
                }]
            }]
        })
    };

    let (status, body) =
        json_request(&app, Method::POST, "/api/v1/register/", register("managed")).await;
    assert_eq!(status, StatusCode::CREATED, "register: {body}");

    // Default delete is refused with 409 Conflict.
    let (status, _) = empty_request(&app, Method::DELETE, "/api/v1/metadata/managed").await;
    assert_eq!(status, StatusCode::CONFLICT);
    // Node still present.
    let (status, _) = empty_request(&app, Method::GET, "/api/v1/metadata/managed").await;
    assert_eq!(status, StatusCode::OK);

    // Forced delete succeeds.
    let (status, _) = empty_request(
        &app,
        Method::DELETE,
        "/api/v1/metadata/managed?external_only=false",
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = empty_request(&app, Method::GET, "/api/v1/metadata/managed").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// Register a plain (asset-free) container `key` under `parent` ("" for root).
async fn register_container(app: &axum::Router, parent: &str, key: &str) {
    let uri = if parent.is_empty() {
        "/api/v1/register/".to_string()
    } else {
        format!("/api/v1/register/{parent}")
    };
    let (status, body) = json_request(
        app,
        Method::POST,
        &uri,
        serde_json::json!({
            "key": key,
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "register {parent}/{key}: {body}"
    );
}

/// `?recursive=true` deletes a non-empty container and its whole subtree in one
/// call (upstream `adapter.py:1069` gates the has-children refusal on
/// `if not recursive`; client `base.py:918-936` sends `recursive`). Boundary:
/// recursive=true over a multi-level subtree.
#[tokio::test]
async fn delete_recursive_removes_whole_subtree() {
    let (app, _dir) = build_test_app().await;
    // expt / expt/a / expt/a/b  (three nested containers).
    register_container(&app, "", "expt").await;
    register_container(&app, "expt", "a").await;
    register_container(&app, "expt/a", "b").await;

    // One DELETE with recursive=true removes the root and every descendant.
    let (status, body) =
        empty_request(&app, Method::DELETE, "/api/v1/metadata/expt?recursive=true").await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "recursive delete: {}",
        String::from_utf8_lossy(&body)
    );

    for path in ["expt", "expt/a", "expt/a/b"] {
        let (status, _) =
            empty_request(&app, Method::GET, &format!("/api/v1/metadata/{path}")).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path} should be gone");
    }
}

/// Boundary: recursive absent / `recursive=false` still refuses a non-empty
/// container with 409 (upstream's default `recursive=False` empty-check).
#[tokio::test]
async fn delete_non_recursive_conflicts_on_nonempty() {
    let (app, _dir) = build_test_app().await;
    register_container(&app, "", "expt").await;
    register_container(&app, "expt", "a").await;

    // No recursive param → 409, message names non-emptiness (not managed data).
    let (status, body) = empty_request(&app, Method::DELETE, "/api/v1/metadata/expt").await;
    assert_eq!(status, StatusCode::CONFLICT, "absent recursive → 409");
    assert!(
        String::from_utf8_lossy(&body).contains("is not empty"),
        "empty-check message: {}",
        String::from_utf8_lossy(&body)
    );

    // Explicit recursive=false → still 409.
    let (status, _) = empty_request(
        &app,
        Method::DELETE,
        "/api/v1/metadata/expt?recursive=false",
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "recursive=false → 409");

    // Node still present.
    let (status, _) = empty_request(&app, Method::GET, "/api/v1/metadata/expt").await;
    assert_eq!(status, StatusCode::OK, "node survives a refused delete");
}

/// Boundary: recursive=true + external_only=true (default) over a subtree that
/// holds an internally-managed data source still refuses, with the
/// `WouldDeleteData` message — recursive skips the empty-check, but the
/// managed-data gate (`delete_node`'s subtree scan) still fires. Before the fix
/// this returned the empty-check 409 instead; the message distinguishes them.
#[tokio::test]
async fn delete_recursive_still_refuses_managed_subtree() {
    let (app, dir) = build_test_app().await;
    let data_uri =
        tiled_rs::core::file_uri::path_to_file_uri(&dir.path().join("frame.h5")).unwrap();

    register_container(&app, "", "root").await;
    // Child array carrying a *writable* (internally-managed) data source.
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/root",
        serde_json::json!({
            "key": "managed",
            "structure_family": "array",
            "metadata": {},
            "specs": [],
            "data_sources": [{
                "structure_family": "array",
                "mimetype": "application/x-hdf5",
                "management": "writable",
                "assets": [{
                    "data_uri": data_uri,
                    "is_directory": false,
                    "parameter": "data_uri"
                }]
            }]
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "register managed child: {body}"
    );

    // recursive=true does NOT override external_only (still default true): the
    // managed data source in the subtree blocks the delete with WouldDeleteData.
    let (status, body) =
        empty_request(&app, Method::DELETE, "/api/v1/metadata/root?recursive=true").await;
    let text = String::from_utf8_lossy(&body);
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "recursive + managed → 409: {text}"
    );
    assert!(
        text.contains("internally managed"),
        "WouldDeleteData message (not empty-check): {text}"
    );

    // Subtree intact.
    let (status, _) = empty_request(&app, Method::GET, "/api/v1/metadata/root/managed").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "managed child survives refused delete"
    );
}

/// F-A: json-patch ops are applied DIRECTLY to each document (metadata ops
/// target the metadata doc, specs ops target the specs array), the body
/// `content-type` discriminator is read from the body (not the transport
/// header), and that field never leaks into the stored metadata.
#[tokio::test]
async fn patch_json_patch_applies_ops_to_each_document() {
    let (app, _dir) = build_test_app().await;
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        serde_json::json!({
            "key": "node",
            "structure_family": "container",
            "metadata": {"a": 1, "b": 2},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // HTTP header is application/json; the real patch type and the ops live
    // in the body. metadata ops add /c and replace /a; specs ops append one.
    let (status, body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/node",
        serde_json::json!({
            "content-type": "application/json-patch+json",
            "metadata": [
                {"op": "add", "path": "/c", "value": 3},
                {"op": "replace", "path": "/a", "value": 9},
            ],
            "specs": [
                {"op": "add", "path": "/-", "value": {"name": "beta"}},
            ],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "patch: {body}");

    let (_, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/node",
        serde_json::Value::Null,
    )
    .await;
    let meta = &body["data"]["attributes"]["metadata"];
    assert_eq!(meta["a"], 9, "replace op applied: {meta}");
    assert_eq!(meta["b"], 2, "unpatched key preserved: {meta}");
    assert_eq!(meta["c"], 3, "add op applied: {meta}");
    assert!(
        meta.get("content-type").is_none(),
        "body content-type leaked into stored metadata: {meta}"
    );
    let specs = body["data"]["attributes"]["specs"].as_array().unwrap();
    assert_eq!(specs.len(), 1, "specs ops applied independently: {specs:?}");
    assert_eq!(specs[0]["name"], "beta");
}

/// F-A: merge-patch merges into the existing metadata document, preserving
/// keys the patch does not mention (the old "partial replace" mode dropped
/// them).
#[tokio::test]
async fn patch_merge_patch_preserves_unpatched_keys() {
    let (app, _dir) = build_test_app().await;
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        serde_json::json!({
            "key": "node",
            "structure_family": "container",
            "metadata": {"a": 1, "b": 2},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/node",
        serde_json::json!({
            "content-type": "application/merge-patch+json",
            "metadata": {"b": 99},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "patch: {body}");

    let (_, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/node",
        serde_json::Value::Null,
    )
    .await;
    let meta = &body["data"]["attributes"]["metadata"];
    assert_eq!(meta["a"], 1, "unpatched key preserved: {meta}");
    assert_eq!(meta["b"], 99, "patched key updated: {meta}");
}

/// F-A/F-K: an unrecognized or absent body `content-type` is rejected with
/// 406 Not Acceptable — there is no silent fallback that overwrites metadata.
#[tokio::test]
async fn patch_unknown_or_missing_content_type_returns_406() {
    let (app, _dir) = build_test_app().await;
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        serde_json::json!({
            "key": "node",
            "structure_family": "container",
            "metadata": {"a": 1},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Unrecognized body content-type.
    let (status, _) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/node",
        serde_json::json!({
            "content-type": "application/garbage",
            "metadata": {"a": 2},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_ACCEPTABLE);

    // Absent body content-type (the old "partial replace" shape).
    let (status, _) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/node",
        serde_json::json!({"metadata": {"a": 3}}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_ACCEPTABLE);

    // The rejected patches must not have mutated the stored metadata.
    let (_, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/node",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(body["data"]["attributes"]["metadata"]["a"], 1);
}

/// F-J: a patch that would push the node over MAX_ALLOWED_SPECS (= 20) is
/// rejected with 422 (Python router.py:2371-2375).
#[tokio::test]
async fn patch_over_limit_specs_returns_422() {
    let (app, _dir) = build_test_app().await;
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        serde_json::json!({
            "key": "node",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // 21 distinct specs (merge-patch replaces the specs array wholesale).
    let too_many: Vec<serde_json::Value> = (0..21)
        .map(|i| serde_json::json!({"name": format!("spec{i}")}))
        .collect();
    let (status, _) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/node",
        serde_json::json!({
            "content-type": "application/merge-patch+json",
            "specs": too_many,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

/// F-J: a patch that would result in non-unique specs is rejected with 422
/// (Python router.py:2376-2380). The two entries below differ in JSON shape
/// but share a `(name, version)` identity, mirroring Python's Spec equality.
#[tokio::test]
async fn patch_duplicate_specs_returns_422() {
    let (app, _dir) = build_test_app().await;
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        serde_json::json!({
            "key": "node",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/node",
        serde_json::json!({
            "content-type": "application/merge-patch+json",
            "specs": [{"name": "x"}, {"name": "x", "version": null}],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
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
    for (key, plan, count) in [("a", "count", 3), ("b", "scan", 7), ("c", "count", 12)] {
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

/// N3: real keyset cursor pagination. Under the default sort the catalog's
/// `search_page` hands back `next_cursor` = the last row's id, and the server
/// emits a `page[cursor]` `next` link. Following that link end-to-end must walk
/// the whole result set exactly once (no gaps, no overlap), report the full
/// match total in `meta.count` on *every* page, and stop (`next` absent) on the
/// last page. Mirrors Python `keys_page`/`_apply_cursor_pagination`.
#[tokio::test]
async fn search_cursor_pagination_walks_keyset() {
    let (app, _dir) = build_test_app().await;

    // Seed five containers in insertion order a..e (default sort = id ASC).
    for key in ["a", "b", "c", "d", "e"] {
        let body = serde_json::json!({
            "key": key,
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        });
        let (status, _) = json_request(&app, Method::POST, "/api/v1/register/", body).await;
        assert_eq!(status, StatusCode::CREATED);
    }

    // Strip the absolute base so the `next` link can be re-issued against the
    // in-process router (which routes on the path + query only).
    let to_relative = |link: &str| link.replace("http://localhost:8000", "");

    // Page 1: an offset request with limit=2. The first window [a, b] comes
    // back, `meta.count` is the full total (5, not the page size), and `next`
    // is a keyset cursor link — NOT page[offset].
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/search/?page[limit]=2",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "page 1: {body}");
    assert_eq!(body["meta"]["count"], 5);
    let ids: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["a", "b"]);
    let next = body["links"]["next"].as_str().expect("page 1 has next");
    assert!(
        next.contains("page[cursor]="),
        "page 1 next is keyset: {next}"
    );

    // Walk every following page by chasing `next` until it is absent. Collect
    // the visited ids to assert full, non-overlapping coverage.
    let mut visited: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
    let mut next_uri = to_relative(next);
    let mut pages = 1;
    loop {
        let (status, body) =
            json_request(&app, Method::GET, &next_uri, serde_json::Value::Null).await;
        assert_eq!(status, StatusCode::OK, "cursor page: {body}");
        pages += 1;
        // A cursor request must echo the cursor in `self` and omit last/prev
        // (a keyset page is forward-only).
        let self_link = body["links"]["self"].as_str().unwrap();
        assert!(
            self_link.contains("page[cursor]="),
            "self is keyset: {self_link}"
        );
        assert!(body["links"]["last"].is_null(), "cursor page has no last");
        assert!(body["links"]["prev"].is_null(), "cursor page has no prev");
        // The total is the full match count on every page, not the page size.
        assert_eq!(body["meta"]["count"], 5);
        for r in body["data"].as_array().unwrap() {
            visited.push(r["id"].as_str().unwrap().to_string());
        }
        match body["links"]["next"].as_str() {
            Some(link) => next_uri = to_relative(link),
            None => break,
        }
    }

    // 5 rows at 2 per page → pages 1,2,3 (last holds the single row `e`).
    assert_eq!(pages, 3, "three pages walked");
    assert_eq!(
        visited,
        vec!["a", "b", "c", "d", "e"],
        "keyset covers all rows once, in order"
    );
}

/// N3: keyset cursors are valid only under the default sort (the id tiebreaker
/// is what makes them stable). A `page[cursor]` combined with an explicit
/// `sort` must be rejected with HTTP 400 — mirrors Python, which only mints
/// cursors for the default ordering.
#[tokio::test]
async fn search_cursor_with_non_default_sort_returns_400() {
    let (app, _dir) = build_test_app().await;
    for key in ["a", "b"] {
        let body = serde_json::json!({
            "key": key,
            "structure_family": "container",
            "metadata": {"plan_name": key},
            "specs": [],
            "data_sources": [],
        });
        let (status, _) = json_request(&app, Method::POST, "/api/v1/register/", body).await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let (status, _) = empty_request(
        &app,
        Method::GET,
        "/api/v1/search/?page[cursor]=1&page[limit]=2&sort=plan_name",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// catalog-M4 Commit 2: searching a NESTED container resolves the parent via
/// the async tree walk (one `CatalogAdapter` per hop) and runs `search_page`
/// against that child's `node_id`. The old direct-SQL branch resolved the
/// parent with a single `lookup`; this confirms the unified trait path queries
/// the right subtree (and only that subtree), with the SQL filter applied.
#[tokio::test]
async fn search_within_nested_container_pushes_filter_to_sql() {
    let (app, _dir) = build_test_app().await;

    // Root container `expt` with two child containers carrying distinct
    // metadata, plus a sibling at root that must NOT appear in the subtree
    // search.
    let register = |path: &str, key: &str, plan: &str| {
        let body = serde_json::json!({
            "key": key,
            "structure_family": "container",
            "metadata": {"plan_name": plan},
            "specs": [],
            "data_sources": [],
        });
        (path.to_string(), body)
    };
    for (path, body) in [
        register("/api/v1/register/", "expt", "root"),
        register("/api/v1/register/expt", "scan_1", "count"),
        register("/api/v1/register/expt", "scan_2", "scan"),
        register("/api/v1/register/", "other", "count"),
    ] {
        let (status, b) = json_request(&app, Method::POST, &path, body).await;
        assert_eq!(status, StatusCode::CREATED, "register {path}: {b}");
    }

    // Search WITHIN expt for plan_name == "count": only expt/scan_1 matches.
    // The root-level `other` (also plan_name=count) must be excluded because
    // the walk scopes the search to the `expt` subtree.
    let url = "/api/v1/search/expt?\
        filter[eq][condition][key]=plan_name&\
        filter[eq][condition][value]=%22count%22";
    let (status, body) = json_request(&app, Method::GET, url, serde_json::Value::Null).await;
    assert_eq!(status, StatusCode::OK, "nested search: {body}");
    let ids: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["scan_1"]);
    assert_eq!(body["meta"]["count"], 1);
    // Container children advertise the default child sort (parity with the
    // metadata endpoint) — the old catalog search branch left this null.
    assert!(body["data"][0]["attributes"]["sorting"].is_array());
}

#[tokio::test]
async fn delete_root_rejected() {
    let (app, _dir) = build_test_app().await;
    let (status, _) = empty_request(&app, Method::DELETE, "/api/v1/metadata/").await;
    // No `*path` segments → 404 from axum routing (no DELETE on the bare
    // collection prefix).
    assert!(matches!(
        status,
        StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
    ));
}

// ---------------------------------------------------------------------------
// M1: select_metadata (JMESPath filtering)
// ---------------------------------------------------------------------------

/// select_metadata on /metadata: returns {"selected": <value>} for the matched field.
#[tokio::test]
async fn select_metadata_extracts_sub_field() {
    let (app, _dir) = build_test_app().await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        serde_json::json!({
            "key": "run1",
            "structure_family": "container",
            "metadata": {"plan_name": "count", "num_points": 5},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/run1?select_metadata=plan_name",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "select_metadata GET: {body}");
    assert_eq!(
        body["data"]["attributes"]["metadata"],
        serde_json::json!({"selected": "count"}),
        "metadata field: {body}"
    );
}

/// select_metadata on /metadata: a malformed JMESPath expression → 400.
#[tokio::test]
async fn select_metadata_malformed_expression_returns_400() {
    let (app, _dir) = build_test_app().await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        serde_json::json!({
            "key": "run2",
            "structure_family": "container",
            "metadata": {"plan_name": "count"},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // "foo[" is an unclosed bracket — always a JMESPath parse error.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/run2?select_metadata=foo%5B",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "expected 400 for malformed expression: {body}"
    );
}

/// select_metadata on /search: each item's metadata is filtered.
#[tokio::test]
async fn select_metadata_on_search_filters_each_item() {
    let (app, _dir) = build_test_app().await;

    for (key, plan) in [("scan1", "count"), ("scan2", "rel_scan")] {
        let (status, _) = json_request(
            &app,
            Method::POST,
            "/api/v1/register/",
            serde_json::json!({
                "key": key,
                "structure_family": "container",
                "metadata": {"plan_name": plan},
                "specs": [],
                "data_sources": [],
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/search/?select_metadata=plan_name",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search select_metadata: {body}");
    let items = body["data"].as_array().expect("data is array");
    assert_eq!(items.len(), 2, "expected 2 items: {body}");
    for item in items {
        let meta = &item["attributes"]["metadata"];
        assert!(
            meta.is_object() && meta["selected"].is_string(),
            "expected {{\"selected\": <string>}}, got {meta}"
        );
    }
}

/// `?fields=` projection (Python `EntryFields`, core.py:248,476,604-620). An
/// absent `fields` returns the full entry; `fields=""` (the Rust client's
/// `keys()` hint, container.rs) returns id-only entries — `ancestors` and the
/// self link kept, every other attribute section dropped; a named projection
/// (`fields=metadata`) keeps only that section. The server used to ignore
/// `fields`, so `keys()` over-fetched full metadata+structure for every child.
#[tokio::test]
async fn fields_projection_on_search_prunes_attributes() {
    let (app, _dir) = build_test_app().await;

    for key in ["a1", "a2"] {
        let (status, _) = json_request(
            &app,
            Method::POST,
            "/api/v1/register/",
            serde_json::json!({
                "key": key,
                "structure_family": "container",
                "metadata": {"plan_name": "count"},
                "specs": [{"name": "s1"}],
                "data_sources": [],
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    // (a) fields="" → id-only: ancestors + self link retained, all other
    // attribute sections dropped.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/search/?fields=",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "fields=\"\": {body}");
    let items = body["data"].as_array().expect("data is array");
    assert_eq!(items.len(), 2, "{body}");
    for item in items {
        let attrs = &item["attributes"];
        assert!(
            attrs.get("ancestors").is_some(),
            "id-only entry keeps ancestors, got {attrs}"
        );
        for dropped in [
            "metadata",
            "structure_family",
            "structure",
            "specs",
            "sorting",
        ] {
            assert!(
                attrs.get(dropped).is_none(),
                "fields=\"\" must drop `{dropped}`, got {attrs}"
            );
        }
        assert!(
            item["links"]["self"].is_string(),
            "id-only entry keeps its self link, got {}",
            item["links"]
        );
    }

    // (b) fields=metadata → keep metadata only; specs/structure_family dropped.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/search/?fields=metadata",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "fields=metadata: {body}");
    for item in body["data"].as_array().unwrap() {
        let attrs = &item["attributes"];
        assert_eq!(attrs["metadata"]["plan_name"], "count", "{attrs}");
        assert!(attrs.get("specs").is_none(), "specs dropped: {attrs}");
        assert!(
            attrs.get("structure_family").is_none(),
            "structure_family dropped: {attrs}"
        );
    }

    // (c) no fields → full entry: metadata AND specs both present.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/search/",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "no fields: {body}");
    for item in body["data"].as_array().unwrap() {
        let attrs = &item["attributes"];
        assert_eq!(attrs["metadata"]["plan_name"], "count");
        assert!(
            attrs.get("specs").is_some(),
            "full entry keeps specs: {attrs}"
        );
    }
}

/// `?include_data_sources=true` on `/search` attaches each entry's
/// `data_sources` list (with assets) to the response, mirroring the single-node
/// metadata route (Python router.py:324 / core.py:483). A node that has data
/// sources gets them populated; a container (none) gets an empty list; and
/// WITHOUT the flag the field is omitted on every entry. The catalog serves this
/// from one batched IN-clause fetch per table over the page's node ids, not a
/// query per row.
#[tokio::test]
async fn search_include_data_sources_populates_and_omits() {
    let (app, dir) = build_test_app().await;
    let data_uri =
        tiled_rs::core::file_uri::path_to_file_uri(&dir.path().join("frame.h5")).unwrap();

    // A leaf array node carrying one data source (with one asset)...
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        serde_json::json!({
            "key": "arr",
            "structure_family": "array",
            "metadata": {"kind": "image"},
            "specs": [],
            "data_sources": [{
                "structure_family": "array",
                "mimetype": "application/x-hdf5",
                "management": "writable",
                "assets": [{
                    "data_uri": data_uri.clone(),
                    "is_directory": false,
                    "parameter": "data_uri"
                }]
            }]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "register arr: {body}");

    // ...and a container node with NO data sources — the mixed page.
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        serde_json::json!({
            "key": "grp",
            "structure_family": "container",
            "metadata": {"kind": "group"},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "register grp: {body}");

    let find = |body: &serde_json::Value, id: &str| -> serde_json::Value {
        body["data"]
            .as_array()
            .expect("data is array")
            .iter()
            .find(|e| e["id"] == id)
            .unwrap_or_else(|| panic!("entry {id} missing: {body}"))
            .clone()
    };

    // (a) With the flag: `arr` carries its data source (+ asset); `grp` (a
    // container with none) carries an empty list.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/search/?include_data_sources=true",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search +ds: {body}");
    assert_eq!(
        body["data"].as_array().unwrap().len(),
        2,
        "two entries: {body}"
    );

    let arr = find(&body, "arr");
    let ds = arr["attributes"]["data_sources"]
        .as_array()
        .unwrap_or_else(|| panic!("arr data_sources not an array: {}", arr["attributes"]));
    assert_eq!(ds.len(), 1, "arr has exactly one data source: {arr}");
    assert_eq!(ds[0]["structure_family"], "array", "ds family: {}", ds[0]);
    assert_eq!(
        ds[0]["mimetype"], "application/x-hdf5",
        "ds mimetype: {}",
        ds[0]
    );
    let assets = ds[0]["assets"].as_array().expect("ds assets is array");
    assert_eq!(assets.len(), 1, "ds carries its one asset: {}", ds[0]);
    assert_eq!(
        assets[0]["data_uri"], data_uri,
        "asset data_uri: {}",
        assets[0]
    );
    assert_eq!(
        assets[0]["parameter"], "data_uri",
        "asset parameter: {}",
        assets[0]
    );

    let grp = find(&body, "grp");
    let grp_ds = grp["attributes"]["data_sources"]
        .as_array()
        .unwrap_or_else(|| {
            panic!(
                "grp data_sources should be an (empty) array, got {}",
                grp["attributes"]
            )
        });
    assert!(grp_ds.is_empty(), "container has empty data_sources: {grp}");

    // (b) Without the flag: `data_sources` omitted on every entry.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/search/",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search no-ds: {body}");
    for item in body["data"].as_array().unwrap() {
        assert!(
            item["attributes"].get("data_sources").is_none(),
            "data_sources omitted without the flag: {}",
            item["attributes"]
        );
    }

    // (c) The `?fields=` projection must NOT strip data_sources — upstream gates
    // it by include_data_sources alone (core.py:483), independent of `fields`.
    // So `fields=metadata&include_data_sources=true` keeps metadata AND data
    // sources while still pruning `specs`.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/search/?fields=metadata&include_data_sources=true",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search fields+ds: {body}");
    let arr = find(&body, "arr");
    assert!(
        arr["attributes"].get("metadata").is_some(),
        "fields=metadata keeps metadata: {arr}"
    );
    assert!(
        arr["attributes"].get("specs").is_none(),
        "fields=metadata prunes specs: {arr}"
    );
    let ds = arr["attributes"]["data_sources"]
        .as_array()
        .unwrap_or_else(|| {
            panic!(
                "data_sources must survive the fields projection, got {}",
                arr["attributes"]
            )
        });
    assert_eq!(ds.len(), 1, "data_sources survive fields projection: {arr}");
}

/// Conditional GET: a JSON metadata response carries a strong (quoted) `ETag`,
/// and a follow-up `If-None-Match` carrying that value returns `304 Not
/// Modified` with an empty body (ETag retained) — activating the client's
/// revalidation cache (tiled-client cache.rs). A non-matching validator falls
/// back to `200` with the full body. Mirrors upstream's md5(content) ETag on
/// metadata responses (core.py:728-735).
#[tokio::test]
async fn metadata_emits_etag_and_honours_if_none_match() {
    let (app, _dir) = build_test_app().await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        serde_json::json!({
            "key": "e1",
            "structure_family": "container",
            "metadata": {"a": 1},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // First GET → 200 with a strong (quoted) ETag.
    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/metadata/e1")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let etag = resp
        .headers()
        .get("etag")
        .expect("response carries an ETag")
        .to_str()
        .unwrap()
        .to_owned();
    assert!(
        etag.starts_with('"') && etag.ends_with('"'),
        "strong quoted ETag, got {etag}"
    );

    // Second GET with the matching validator → 304, empty body, ETag retained.
    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/metadata/e1")
        .header("if-none-match", &etag)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(
        resp.headers().get("etag").and_then(|v| v.to_str().ok()),
        Some(etag.as_str()),
        "304 retains the ETag validator"
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(
        body.is_empty(),
        "304 carries no body, got {} bytes",
        body.len()
    );

    // A stale validator → full 200 response again.
    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/metadata/e1")
        .header("if-none-match", "\"stale\"")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "non-matching validator → 200"
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(!body.is_empty(), "200 carries the full body");
}

/// Server M1: `PUT /api/v1/metadata/{path}` wholesale-replaces metadata + specs
/// (distinct from PATCH's partial json-patch/merge-patch), unblocking the Python
/// client's `replace_metadata()` (405 today). Mirrors Python `put_metadata`
/// (server/router.py:2420-2494): a present field replaces wholesale, an absent /
/// null field keeps the current value, and — since this crate has no access
/// policy exposing `modify_node` — a sent access_blob is NOT applied but is
/// echoed back to signal the rejection (router.py:2484-2487).
#[tokio::test]
async fn put_metadata_replaces_wholesale_and_signals_unapplied_access_blob() {
    let (app, _dir) = build_test_app().await;

    // Create a container carrying two metadata keys and one spec.
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "n1",
            "structure_family": "container",
            "metadata": {"a": 1, "old": 2},
            "specs": [{"name": "s1"}],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create: {body}");

    // (a) PUT replaces metadata + specs wholesale. No access_blob in the body →
    // the response is just `{id}` (no access_blob key).
    let (status, body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/n1",
        serde_json::json!({"metadata": {"a": 9}, "specs": [{"name": "s2"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "put replace: {body}");
    assert_eq!(body["id"], "n1");
    assert!(
        body.get("access_blob").is_none(),
        "no access_blob was sent → response must omit it, got {body}"
    );

    // The replacement is wholesale: the dropped key "old" is gone, and the spec
    // list is the new one.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/n1",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["data"]["attributes"]["metadata"],
        serde_json::json!({"a": 9})
    );
    let specs = body["data"]["attributes"]["specs"].as_array().unwrap();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0]["name"], "s2");

    // (b) PUT with ONLY access_blob: metadata/specs are absent → kept; the sent
    // access_blob is not applied but IS echoed back (key present) to signal the
    // rejection.
    let (status, body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/n1",
        serde_json::json!({"access_blob": {"role": "secret"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "put access_blob: {body}");
    assert_eq!(body["id"], "n1");
    assert!(
        body.as_object().unwrap().contains_key("access_blob"),
        "a differing access_blob was sent → response must echo the unchanged value, got {body}"
    );
    assert_ne!(
        body["access_blob"],
        serde_json::json!({"role": "secret"}),
        "the sent access_blob must NOT have been applied"
    );

    // metadata/specs untouched by the access_blob-only PUT.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/n1",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["data"]["attributes"]["metadata"],
        serde_json::json!({"a": 9})
    );

    // (c) Too many specs → 422, never written.
    let many: Vec<serde_json::Value> = (0..21)
        .map(|i| serde_json::json!({"name": format!("s{i}")}))
        .collect();
    let (status, _body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/n1",
        serde_json::json!({"specs": many}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "21 specs must be 422"
    );

    let _ = empty_request(&app, Method::DELETE, "/api/v1/metadata/n1").await;
}

/// Server M2: `GET /api/v1/revisions/{path}` lists a node's metadata history and
/// `DELETE .../{path}?number=N` drops one. The revisions table is populated by
/// every metadata update (PUT/PATCH without ?drop_revision). Mirrors Python
/// get_revisions / delete_revision (router.py:2496-2535) and
/// construct_revisions_response (core.py:330-353): each item is
/// `{revision_number, attributes: {metadata, specs, time_updated}}`, ascending
/// by revision; a missing revision DELETE is 404.
#[tokio::test]
async fn revisions_list_then_delete_round_trip() {
    let (app, _dir) = build_test_app().await;

    // Create a node, then update it twice — each PUT pushes the PRE-update
    // state onto the revisions table (revision 1 = {"v":1}, revision 2 = {"v":2}).
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "rev1",
            "structure_family": "container",
            "metadata": {"v": 1},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    for v in [2, 3] {
        let (status, _) = json_request(
            &app,
            Method::PUT,
            "/api/v1/metadata/rev1",
            serde_json::json!({"metadata": {"v": v}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "put v={v}");
    }

    // GET lists both revisions, ascending, carrying the superseded metadata.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/revisions/rev1",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "get revisions: {body}");
    assert_eq!(body["meta"]["count"], 2);
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["revision_number"], 1);
    assert_eq!(
        data[0]["attributes"]["metadata"],
        serde_json::json!({"v": 1})
    );
    assert_eq!(data[1]["revision_number"], 2);
    assert_eq!(
        data[1]["attributes"]["metadata"],
        serde_json::json!({"v": 2})
    );
    assert!(
        data[0]["attributes"]["time_updated"].is_string(),
        "time_updated must be present: {body}"
    );
    assert!(
        body["links"]["self"]
            .as_str()
            .unwrap()
            .contains("/revisions/rev1"),
        "self link points at the revisions route: {body}"
    );

    // DELETE revision 1 → 200; the list then holds only revision 2.
    let (status, _) = empty_request(&app, Method::DELETE, "/api/v1/revisions/rev1?number=1").await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/revisions/rev1",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["meta"]["count"], 1);
    assert_eq!(body["data"][0]["revision_number"], 2);

    // DELETE a non-existent revision → 404.
    let (status, _) = empty_request(&app, Method::DELETE, "/api/v1/revisions/rev1?number=99").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let _ = empty_request(&app, Method::DELETE, "/api/v1/metadata/rev1").await;
}

/// A node with more revisions than one page must report the TOTAL revision
/// count in `meta.count` and emit working `next`/`last` pagination links so a
/// client can walk every revision one page at a time. Before upstream #1409
/// the endpoint fed the page length as the count, so `pagination_links`
/// derived `last_offset = 0`, never emitted a `next` link, and revisions past
/// page 1 were unreachable. Regression for that defect (fails on old behavior).
#[tokio::test]
async fn revisions_pagination_walks_all_pages() {
    let (app, _dir) = build_test_app().await;

    // Create with {v:1}; three PUTs push revisions 1,2,3 (each PUT pushes the
    // PRE-update state, as in `revisions_list_then_delete_round_trip`).
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "pager",
            "structure_family": "container",
            "metadata": {"v": 1},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    for v in [2, 3, 4] {
        let (status, _) = json_request(
            &app,
            Method::PUT,
            "/api/v1/metadata/pager",
            serde_json::json!({"metadata": {"v": v}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "put v={v}");
    }

    // First page (limit=1): total count is 3 (not the page length 1), one item,
    // `next` + `last` present, `last` pointing at the final page (offset 2).
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/revisions/pager?page[offset]=0&page[limit]=1",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["meta"]["count"], 3,
        "count is the node total, not the page length: {body}"
    );
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert!(
        body["links"]["next"].is_string(),
        "next must be present when more revisions remain: {body}"
    );
    assert!(
        body["links"]["last"]
            .as_str()
            .unwrap()
            .contains("page[offset]=2"),
        "last points at the final page (offset 2): {body}"
    );

    // Walk every page via the `next` links and confirm all three revisions come
    // back in order with no overlap.
    let mut revision_numbers = Vec::new();
    let mut next = Some("/api/v1/revisions/pager?page[offset]=0&page[limit]=1".to_string());
    while let Some(uri) = next {
        let (status, body) = json_request(&app, Method::GET, &uri, serde_json::Value::Null).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        for item in body["data"].as_array().unwrap() {
            revision_numbers.push(item["revision_number"].as_i64().unwrap());
        }
        next = body["links"]["next"]
            .as_str()
            .map(|link| link.replace("http://localhost:8000", ""));
    }
    assert_eq!(revision_numbers, vec![1, 2, 3]);
}

/// Boundary cases for the revisions count/pagination: exactly one full page,
/// an empty node (zero revisions), and an offset past the end. In every case
/// `meta.count` is the node's total and `next` is absent (no page remains).
#[tokio::test]
async fn revisions_pagination_boundaries() {
    let (app, _dir) = build_test_app().await;

    // (a) Exactly one page: 2 revisions, page[limit]=2 holds both → count=2,
    //     next=None (offset+limit == count), last collapses to offset 0.
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "onepage",
            "structure_family": "container",
            "metadata": {"v": 1},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    for v in [2, 3] {
        let (status, _) = json_request(
            &app,
            Method::PUT,
            "/api/v1/metadata/onepage",
            serde_json::json!({"metadata": {"v": v}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/revisions/onepage?page[offset]=0&page[limit]=2",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["meta"]["count"], 2);
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
    assert!(
        body["links"]["next"].is_null(),
        "no next on a single full page: {body}"
    );

    // (b) Zero revisions: a freshly created node (never PUT) has none →
    //     count=0, empty page, next=None.
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "norevs",
            "structure_family": "container",
            "metadata": {"v": 1},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/revisions/norevs",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["meta"]["count"], 0);
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
    assert!(body["links"]["next"].is_null());

    // (c) Offset past the end of a non-empty list: count is still the total (2),
    //     the page is empty, next=None. Mirrors upstream
    //     test_metadata_revisions_count_empty case (b).
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/revisions/onepage?page[offset]=10",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["meta"]["count"], 2,
        "offset past end still reports the total: {body}"
    );
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
    assert!(body["links"]["next"].is_null());
}

/// exact_count_limit caps the `meta.count` returned by the search endpoint.
/// With limit = 2 and 5 nodes registered, `meta.count` must be 2 not 5.
/// Mirrors Python `Settings.exact_count_limit` (settings.py, default 100).
#[tokio::test]
async fn exact_count_limit_caps_meta_count() {
    // Build a fresh app with a tight exact_count_limit.
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: tiled_rs::core::queries::Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: Some("http://localhost:8000".into()),
        cors_policy: tiled_rs::server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: Some(catalog),
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
        exact_count_limit: 2,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    };
    let app = tiled_rs::server::build_app(state);

    // Register 5 nodes — more than the limit of 2.
    for key in ["a", "b", "c", "d", "e"] {
        let body = serde_json::json!({
            "key": key,
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        });
        let (status, _) = json_request(&app, Method::POST, "/api/v1/register/", body).await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/search/",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["meta"]["count"], 2,
        "meta.count must be capped at exact_count_limit=2; got {}",
        body["meta"]["count"]
    );
}

/// catalog #1096 follow-up: `exact_count_limit` now also drives the
/// per-entry container counts inside a search-results page (previously it
/// only capped the envelope `meta.count`). Threads a tight limit through
/// `CatalogAdapter::with_exact_count_limit` — the same builder call
/// `src/cli/mod.rs` now makes with `AppState.exact_count_limit` — and
/// verifies each listed container's `attributes.structure.count` is still
/// exact and correct: one past the threshold, one under it, one with zero
/// children (the batched `GROUP BY` "absent from the map" case).
#[tokio::test]
async fn exact_count_limit_threads_into_search_page_entry_counts() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    // limit=1: smaller than "busy"'s child count below, so on Postgres this
    // would take the lower-bound/approximate path; on SQLite it must have
    // no effect on correctness (count_children_or_approx is unconditionally
    // exact there).
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(
        tiled_rs::catalog::CatalogAdapter::root(catalog.clone(), resolver)
            .with_exact_count_limit(1),
    );
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: tiled_rs::core::queries::Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: Some("http://localhost:8000".into()),
        cors_policy: tiled_rs::server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: Some(catalog),
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
        exact_count_limit: 1,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    };
    let app = tiled_rs::server::build_app(state);

    // "busy": 5 children (past the limit=1 threshold).
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        serde_json::json!({
            "key": "busy", "structure_family": "container",
            "metadata": {}, "specs": [], "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    for i in 0..5 {
        let (status, _) = json_request(
            &app,
            Method::POST,
            "/api/v1/register/busy",
            serde_json::json!({
                "key": format!("child_{i}"), "structure_family": "container",
                "metadata": {}, "specs": [], "data_sources": [],
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    // "empty": 0 children.
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        serde_json::json!({
            "key": "empty", "structure_family": "container",
            "metadata": {}, "specs": [], "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/search/",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let entries = body["data"].as_array().expect("data must be an array");
    let count_of = |key: &str| -> i64 {
        entries
            .iter()
            .find(|e| e["id"] == key)
            .unwrap_or_else(|| panic!("entry '{key}' missing from search response"))
            ["attributes"]["structure"]["count"]
            .as_i64()
            .unwrap_or_else(|| panic!("entry '{key}' has no structure.count"))
    };
    assert_eq!(
        count_of("busy"),
        5,
        "busy's per-entry count must be exact (5), not capped at exact_count_limit=1"
    );
    assert_eq!(
        count_of("empty"),
        0,
        "empty container's per-entry count must be 0, not miscounted by the batched query"
    );
}
