//! DELETE /api/v1/metadata must require BOTH `delete:node` AND `delete:revision`
//! on the fully-narrowed node context, matching upstream router.py:1995 (global
//! `Security(check_scopes, ["delete:node","delete:revision"])`) + :1999
//! (`get_entry(path, ["delete:node","delete:revision"], ...)`). Deleting a node
//! cascade-destroys its revision history, so `delete:revision` is required too.
//!
//! The built-in roles bundle the two scopes (`for_role` gives every
//! `delete:node` holder `delete:revision`), so no default principal is affected.
//! This gate is only reachable via a custom AccessPolicy that grants
//! `delete:node` WITHOUT `delete:revision` on a node — exactly what
//! `SplitDeleteScopePolicy` does for the `restricted` node here.
//!
//! Pre-fix (single-scope gate) the DELETE succeeds (204, cascading away the
//! revisions); after the fix it is refused (403). A positive control on the
//! `allowed` node — where the policy grants both scopes — stays 204.

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::access::{AccessPolicy, Decision, NodeContext, Principal, Scope, ScopeSet};
use tiled_rs::auth::{AuthDb, DummyAuthenticator, Issuer};
use tiled_rs::catalog::{Catalog, node::RegisterRequest};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

/// Grants the admin its full session scopes everywhere EXCEPT the `restricted`
/// node, where it returns `{read:metadata, delete:node}` — deliberately
/// omitting `delete:revision`. Models a custom policy that splits the two
/// delete scopes, the only configuration that reaches this gate.
struct SplitDeleteScopePolicy;

#[async_trait]
impl AccessPolicy for SplitDeleteScopePolicy {
    async fn anonymous_decision(&self, _ctx: NodeContext<'_>) -> Decision {
        Decision {
            scopes: ScopeSet::new(),
        }
    }

    async fn principal_decision(
        &self,
        _principal: &Principal,
        session_scopes: &ScopeSet,
        _authn_access_tags: Option<&[String]>,
        ctx: NodeContext<'_>,
    ) -> Decision {
        if ctx.path.first().map(String::as_str) == Some("restricted") {
            Decision {
                scopes: ScopeSet::from_iter([Scope::ReadMetadata, Scope::DeleteNode]),
            }
        } else {
            Decision {
                scopes: session_scopes.clone(),
            }
        }
    }
}

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

    // `root` promoted to admin so its session scopes carry both delete scopes;
    // the policy is what strips delete:revision on `restricted`.
    let (root, _) = auth_db.ensure_principal("dummy", "root").await.unwrap();
    auth_db
        .update_principal_role(root.id, "admin")
        .await
        .unwrap();

    // Two empty containers, both deletable (no children, no managed data).
    for key in ["restricted", "allowed"] {
        catalog
            .create_node(
                None,
                vec![],
                RegisterRequest {
                    key: key.to_string(),
                    structure_family: "container".to_string(),
                    metadata: json!({}),
                    specs: json!([]),
                    access_blob: json!({}),
                },
            )
            .await
            .unwrap();
    }

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

    let access_policy: Arc<dyn AccessPolicy> = Arc::new(SplitDeleteScopePolicy);

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
        access_policy: Some(access_policy),
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

async fn delete(app: &axum::Router, bearer: &str, key: &str) -> StatusCode {
    let req = Request::builder()
        .method(Method::DELETE)
        .uri(format!("/api/v1/metadata/{key}"))
        .header("authorization", bearer)
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(req).await.unwrap().status()
}

/// The policy grants delete:node but NOT delete:revision on `restricted`.
/// Deleting the node would cascade-destroy its revisions, so it must be
/// refused. FAILS on the current (single-scope) tree, where the DELETE
/// succeeds (204).
#[tokio::test]
async fn delete_metadata_denied_without_delete_revision() {
    let app = build_app().await;
    let bearer = login(&app).await;
    let status = delete(&app, &bearer, "restricted").await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "delete without delete:revision must be refused, got {status}"
    );
}

/// Positive control: on `allowed` the policy grants the full scope set (both
/// delete scopes), so the DELETE still succeeds. Stays 204 pre- and post-fix,
/// proving the fix only tightens the scope-split case.
#[tokio::test]
async fn delete_metadata_allowed_with_both_delete_scopes() {
    let app = build_app().await;
    let bearer = login(&app).await;
    let status = delete(&app, &bearer, "allowed").await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "delete with both delete scopes must still succeed, got {status}"
    );
}
