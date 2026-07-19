//! When the wired `AccessPolicy` REJECTS a provided `access_blob`, the create
//! (`POST /register`), PATCH, and PUT metadata routes must answer **403**, not
//! 422. Upstream catches the policy's `ValueError` from `init_node` /
//! `modify_node` and raises `HTTPException(HTTP_403_FORBIDDEN, "Access policy
//! rejects the provided access blob.\n{e}")` at router.py:1896-1899 (create),
//! :2401-2403 (PATCH), and :2477-2479 (PUT).
//!
//! `RejectBlobPolicy` grants full scopes (so the caller clears the route-scope
//! and per-node gates and actually reaches the policy hook) but fails every
//! `init_node` / `modify_node` call, isolating the blob-rejection status code.

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

/// Grants full scopes at every node (so the caller passes both authorization
/// gates) but rejects the access blob on every write, mirroring a tag policy
/// that refuses a blob the caller is not entitled to set.
struct RejectBlobPolicy;

#[async_trait]
impl AccessPolicy for RejectBlobPolicy {
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

    async fn init_node(
        &self,
        _principal: &Principal,
        _authn_access_tags: Option<&[String]>,
        _session_scopes: &ScopeSet,
        _access_blob: Option<&serde_json::Value>,
    ) -> Result<(bool, serde_json::Value), String> {
        Err("caller may not set this access blob".into())
    }

    async fn modify_node(
        &self,
        _node_access_blob: &serde_json::Value,
        _principal: &Principal,
        _authn_access_tags: Option<&[String]>,
        _session_scopes: &ScopeSet,
        _proposed_access_blob: Option<&serde_json::Value>,
    ) -> Result<(bool, serde_json::Value), String> {
        Err("caller may not set this access blob".into())
    }
}

/// Build an app with `RejectBlobPolicy` wired in, a `root` principal promoted to
/// admin (so it holds write:metadata + create:node + register), and one seeded
/// container `node` to PATCH/PUT.
async fn build_app() -> axum::Router {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    // Pool size 1: this is a login+write test; a single warmed connection avoids
    // the SQLite cold-start (error 14) race that flakes such tests under nextest.
    let catalog = Catalog::connect_with_pool_size(&cat_uri, 1).await.unwrap();
    catalog.migrate().await.unwrap();
    let auth_db = AuthDb::connect_with_pool_size(&auth_uri, 1).await.unwrap();
    auth_db.migrate().await.unwrap();

    let (root, _) = auth_db.ensure_principal("dummy", "root").await.unwrap();
    auth_db
        .update_principal_role(root.id, "admin")
        .await
        .unwrap();

    catalog
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: "node".to_string(),
                structure_family: "container".to_string(),
                metadata: json!({}),
                specs: json!([]),
                access_blob: json!({}),
            },
        )
        .await
        .unwrap();

    let access_policy: Arc<dyn AccessPolicy> = Arc::new(RejectBlobPolicy);
    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> =
        Arc::new(tiled_rs::catalog::adapter::UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let mut dummy = DummyAuthenticator::new("dummy");
    dummy.add_user("root", "toor").unwrap();

    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: Some("http://localhost:8000".into()),
        root_path: String::new(),
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
        allow_anonymous_access: false,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
        validation: Default::default(),
    };
    tiled_rs::server::build_app(state)
}

async fn login(app: &axum::Router) -> String {
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/dummy/login")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"username": "root", "password": "toor"})).unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    format!("Bearer {}", body["access_token"].as_str().unwrap())
}

async fn send(
    app: &axum::Router,
    method: Method,
    uri: &str,
    bearer: &str,
    body: serde_json::Value,
) -> StatusCode {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", bearer)
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    app.clone().oneshot(req).await.unwrap().status()
}

/// POST /register with a rejected access_blob → 403 (policy `init_node` fails).
#[tokio::test]
async fn create_with_rejected_access_blob_is_403() {
    let app = build_app().await;
    let bearer = login(&app).await;
    let status = send(
        &app,
        Method::POST,
        "/api/v1/register/",
        &bearer,
        json!({
            "key": "newnode",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
            "access_blob": {"tags": ["secret"]},
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "create with a policy-rejected access blob must be 403 (upstream router.py:1896)"
    );
}

/// PATCH /metadata with a rejected access_blob → 403 (policy `modify_node` fails).
#[tokio::test]
async fn patch_with_rejected_access_blob_is_403() {
    let app = build_app().await;
    let bearer = login(&app).await;
    let status = send(
        &app,
        Method::PATCH,
        "/api/v1/metadata/node",
        &bearer,
        json!({
            "content-type": "application/merge-patch+json",
            "access_blob": {"tags": ["secret"]},
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "PATCH with a policy-rejected access blob must be 403 (upstream router.py:2401)"
    );
}

/// PUT /metadata with a rejected access_blob → 403 (policy `modify_node` fails).
#[tokio::test]
async fn put_with_rejected_access_blob_is_403() {
    let app = build_app().await;
    let bearer = login(&app).await;
    let status = send(
        &app,
        Method::PUT,
        "/api/v1/metadata/node",
        &bearer,
        json!({
            "metadata": {},
            "specs": [],
            "access_blob": {"tags": ["secret"]},
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "PUT with a policy-rejected access blob must be 403 (upstream router.py:2477)"
    );
}
