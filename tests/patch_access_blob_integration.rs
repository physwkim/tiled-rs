//! PATCH /api/v1/metadata must apply the json-patch / merge-patch step to
//! `access_blob` (base = the node's stored blob) BEFORE handing it to the
//! access policy's `modify_node` — exactly as it already does for `metadata`
//! and `specs`. Upstream router.py:2351 (json-patch) / :2364-2367 (merge-patch)
//! feed the *patched* blob to the policy at :2397.
//!
//! An `EchoPolicy` whose `modify_node` persists whatever it receives makes the
//! stored `access_blob` reveal exactly what the handler passed in.

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::access::{AccessPolicy, Decision, NodeContext, Principal, ScopeSet};
use tiled_rs::auth::{AuthDb, DummyAuthenticator, Issuer};
use tiled_rs::catalog::{Catalog, node::RegisterRequest};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

/// Test policy: grants full scopes and persists whatever `modify_node`
/// receives as the proposed blob, so the stored `access_blob` after a PATCH is
/// exactly the value the handler handed to the policy.
struct EchoPolicy;

#[async_trait]
impl AccessPolicy for EchoPolicy {
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

    async fn modify_node(
        &self,
        node_access_blob: &serde_json::Value,
        _principal: &Principal,
        _authn_access_tags: Option<&[String]>,
        _session_scopes: &ScopeSet,
        proposed_access_blob: Option<&serde_json::Value>,
    ) -> Result<(bool, serde_json::Value), String> {
        match proposed_access_blob {
            Some(p) => Ok((true, p.clone())),
            None => Ok((false, node_access_blob.clone())),
        }
    }
}

/// Build an app with `EchoPolicy` wired in, a dummy authenticator, and one
/// seeded container `node` carrying a two-key `access_blob`.
async fn build_app_with_echo_policy(seed_access_blob: serde_json::Value) -> axum::Router {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    // Pool size 1 (not the default 8/16) is deliberate: under `cargo nextest`
    // every test is its own process, and many cold-start their SQLite pools at
    // once on a small CI runner. A fresh pool that opens a *new* WAL connection
    // while the box is saturated intermittently gets SQLite error 14 ("unable to
    // open database file"), which this write-path test surfaces as a spurious
    // 500/401. With a single connection, migrate()/create_node() below warm it
    // and every later request reuses it — no connection is opened mid-request,
    // so the cold-start race cannot fire. The tests issue one request at a time,
    // so a single connection never contends.
    let catalog = Catalog::connect_with_pool_size(&cat_uri, 1).await.unwrap();
    catalog.migrate().await.unwrap();
    let auth_db = AuthDb::connect_with_pool_size(&auth_uri, 1).await.unwrap();
    auth_db.migrate().await.unwrap();
    auth_db.ensure_principal("dummy", "alice").await.unwrap();

    catalog
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: "node".to_string(),
                structure_family: "container".to_string(),
                metadata: json!({}),
                specs: json!([]),
                access_blob: seed_access_blob,
            },
        )
        .await
        .unwrap();

    let access_policy: Arc<dyn AccessPolicy> = Arc::new(EchoPolicy);
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
        streaming_cache: tiled_rs::server::streaming_cache::disabled(),
        access_policy: Some(access_policy),
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
        enable_web: true,
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

async fn login(app: &axum::Router) -> String {
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/dummy/login")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"username": "alice", "password": "wonderland"})).unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    format!("Bearer {}", body["access_token"].as_str().unwrap())
}

async fn patch(
    app: &axum::Router,
    bearer: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let req = Request::builder()
        .method(Method::PATCH)
        .uri("/api/v1/metadata/node")
        .header("content-type", "application/json")
        .header("authorization", bearer)
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

/// merge-patch: a one-key patch must MERGE into the stored blob — the policy
/// receives `{"a":1,"b":9}`, not the raw `{"b":9}` (which would drop `a`).
#[tokio::test]
async fn patch_merge_access_blob_preserves_untouched_keys() {
    let app = build_app_with_echo_policy(json!({"a": 1, "b": 2})).await;
    let bearer = login(&app).await;

    let (status, body) = patch(
        &app,
        &bearer,
        json!({
            "content-type": "application/merge-patch+json",
            "access_blob": {"b": 9},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "patch: {body}");

    // What modify_node received (and EchoPolicy persisted) = the merged blob.
    assert_eq!(
        body["access_blob"],
        json!({"a": 1, "b": 9}),
        "merge-patch must merge into the stored blob, not replace it: {body}"
    );
}

/// json-patch: an ops array must be APPLIED to the stored blob — the policy
/// receives `{"a":1,"b":2,"c":3}`, not the raw ops array.
#[tokio::test]
async fn patch_json_patch_access_blob_applies_ops() {
    let app = build_app_with_echo_policy(json!({"a": 1, "b": 2})).await;
    let bearer = login(&app).await;

    let (status, body) = patch(
        &app,
        &bearer,
        json!({
            "content-type": "application/json-patch+json",
            "access_blob": [{"op": "add", "path": "/c", "value": 3}],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "patch: {body}");

    assert_eq!(
        body["access_blob"],
        json!({"a": 1, "b": 2, "c": 3}),
        "json-patch ops must be applied before the policy sees the blob: {body}"
    );
}
