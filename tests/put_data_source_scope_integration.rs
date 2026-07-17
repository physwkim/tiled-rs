//! PUT /api/v1/data_source rewrites a node's storage mapping (structure +
//! parameters). Upstream router.py:1931-1974 gates it with BOTH
//! `write:metadata` AND `register` — `Security(check_scopes, ["write:metadata",
//! "register"])` (:1944) and `get_entry(path, ["write:metadata","register"])`
//! (:1948) — so a plain `user` (which lacks `register`) is refused.
//!
//! These tests pin the scope contract at the HTTP boundary: a default
//! `user`-role principal (write:data + write:metadata, no register) is denied
//! (403), while an `admin` (which carries register) succeeds (200). With no
//! access policy wired, session scopes are exactly `for_role(role)`, so the
//! only thing under test is the handler's scope gate.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::auth::{AuthDb, DummyAuthenticator, Issuer};
use tiled_rs::catalog::data_source::DataSourceSpec;
use tiled_rs::catalog::{Catalog, node::RegisterRequest};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

/// Build an app with a catalog-backed root, a dummy authenticator, and one
/// seeded array node `arr` carrying a single data_source. Returns the router
/// and the id of that data_source (the target of the PUT rewrite).
///
/// `access_policy` is intentionally `None`: with no policy, `narrow_for_node`
/// leaves session scopes untouched, so each principal's effective scopes at
/// the node are exactly `for_role(role)` — isolating the handler's own scope
/// gate as the sole thing under test.
async fn build_app() -> (axum::Router, i64) {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    // Pool size 1 (not the default 8/16) is deliberate: under `cargo nextest`
    // every test is its own process, and many cold-start their SQLite pools at
    // once on a small CI runner. A fresh pool that opens a *new* WAL connection
    // while the box is saturated intermittently gets SQLite error 14 ("unable
    // to open database file"), which this login+write test would surface as a
    // spurious 500/401. With a single connection, migrate()/create_node() below
    // warm it and every later request reuses it — no connection is opened
    // mid-request, so the cold-start race cannot fire. The tests issue one
    // request at a time, so a single connection never contends.
    let catalog = Catalog::connect_with_pool_size(&cat_uri, 1).await.unwrap();
    catalog.migrate().await.unwrap();
    let auth_db = AuthDb::connect_with_pool_size(&auth_uri, 1).await.unwrap();
    auth_db.migrate().await.unwrap();

    // `alice` keeps the default `user` role (no register scope). `root` is
    // promoted to `admin` (full scopes, register included).
    auth_db.ensure_principal("dummy", "alice").await.unwrap();
    let (root, _) = auth_db.ensure_principal("dummy", "root").await.unwrap();
    auth_db
        .update_principal_role(root.id, "admin")
        .await
        .unwrap();

    let node = catalog
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: "arr".to_string(),
                structure_family: "array".to_string(),
                metadata: json!({}),
                specs: json!([]),
                access_blob: json!({}),
            },
        )
        .await
        .unwrap();
    let ds = catalog
        .create_data_source(
            node.id,
            DataSourceSpec {
                structure_family: "array".into(),
                structure: json!({
                    "shape": [10],
                    "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
                    "chunks": [[10]],
                }),
                mimetype: "application/x-hdf5".into(),
                parameters: json!({}),
                management: "external".into(),
                assets: vec![],
            },
        )
        .await
        .unwrap();

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> =
        Arc::new(tiled_rs::catalog::adapter::UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let mut dummy = DummyAuthenticator::new("dummy");
    dummy.add_user("alice", "wonderland").unwrap();
    dummy.add_user("root", "toor").unwrap();

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
        auth_db: Some(auth_db),
        issuer: Some(issuer),
        authenticators: vec![Arc::new(dummy)],
        proxied_header_auth: None,
        external_oidc: None,
        #[cfg(feature = "saml")]
        saml_providers: vec![],
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_rs::server::streaming::StreamingBus::new(),
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
    (tiled_rs::server::build_app(state), ds.id)
}

async fn login(app: &axum::Router, username: &str, password: &str) -> String {
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/dummy/login")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"username": username, "password": password})).unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    format!("Bearer {}", body["access_token"].as_str().unwrap())
}

/// A rewrite of the data_source's structure + parameters — the storage
/// mapping mutation the scope gate is meant to protect.
fn rewrite_body(ds_id: i64) -> serde_json::Value {
    json!({
        "data_source": {
            "id": ds_id,
            "structure": {
                "shape": [20],
                "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
                "chunks": [[20]],
            },
            "parameters": {"rewritten": true},
        }
    })
}

async fn put_data_source(
    app: &axum::Router,
    bearer: &str,
    ds_id: i64,
) -> (StatusCode, serde_json::Value) {
    let req = Request::builder()
        .method(Method::PUT)
        .uri("/api/v1/data_source/arr")
        .header("content-type", "application/json")
        .header("authorization", bearer)
        .body(Body::from(
            serde_json::to_vec(&rewrite_body(ds_id)).unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
}

/// A default `user` (write:data + write:metadata, no register) must be REFUSED:
/// rewriting a storage mapping is a register-scoped operation upstream.
#[tokio::test]
async fn put_data_source_denied_for_user_without_register() {
    let (app, ds_id) = build_app().await;
    let bearer = login(&app, "alice", "wonderland").await;

    let (status, body) = put_data_source(&app, &bearer, ds_id).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "user without `register` must be denied PUT /data_source: {body}"
    );
}

/// An `admin` carries `register`, so the rewrite succeeds — the fix must not
/// regress the legitimate path.
#[tokio::test]
async fn put_data_source_allowed_for_admin_with_register() {
    let (app, ds_id) = build_app().await;
    let bearer = login(&app, "root", "toor").await;

    let (status, body) = put_data_source(&app, &bearer, ds_id).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "admin with `register` must be allowed PUT /data_source: {body}"
    );
    assert_eq!(
        body["data_source"]["parameters"],
        json!({"rewritten": true}),
        "the rewrite must have been applied: {body}"
    );
}
