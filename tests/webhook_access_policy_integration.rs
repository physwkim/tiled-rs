//! The four webhook routes must apply the per-node access-policy narrow, not
//! just the global scope check. Upstream (webhook_router.py) gates each route
//! with `Security(check_scopes, [scope])` PLUS `get_entry(path, [scope],
//! access_policy=...)`: register :157/:159-170, list :218/:220-231, delete
//! :254/:269-280, history :301/:316-327. So a webhooks-scope holder that the
//! wired AccessPolicy node-restricts from subtree B cannot POST/GET/DELETE or
//! read history for B's webhooks.
//!
//! Webhook scopes are admin-only (`for_role`), so the restricted holder here is
//! an admin whom a custom policy denies on one subtree. `SubtreeDenyPolicy`
//! grants that admin full scopes everywhere EXCEPT under `secret`, where it
//! returns an empty scope set (no `read:metadata`) — the node is invisible.
//!
//! Pre-fix: all four routes reach `secret`'s webhooks (200). After the fix each
//! is refused (404, the missing-`read:metadata` form of get_entry's denial). A
//! positive control on the unrestricted `open` node stays 200 throughout, so
//! the fix is a per-node narrow, not a blanket block.

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::access::{AccessPolicy, Decision, NodeContext, Principal, ScopeSet};
use tiled_rs::auth::{AuthDb, DummyAuthenticator, Issuer};
use tiled_rs::catalog::webhook::WebhookCreate;
use tiled_rs::catalog::{Catalog, node::RegisterRequest};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

/// Grants the principal its full session scopes on every node EXCEPT any node
/// whose top path segment is `denied`, where it returns an empty scope set —
/// modelling a custom policy that hides one subtree from an otherwise-admin
/// principal.
struct SubtreeDenyPolicy {
    denied: String,
}

#[async_trait]
impl AccessPolicy for SubtreeDenyPolicy {
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
        if ctx.path.first().map(String::as_str) == Some(self.denied.as_str()) {
            Decision {
                scopes: ScopeSet::new(),
            }
        } else {
            Decision {
                scopes: session_scopes.clone(),
            }
        }
    }
}

/// Ids of webhooks pre-seeded (bypassing the routes) on each node, so the
/// by-id routes (delete/history) have real targets.
struct Seeded {
    app: axum::Router,
    wh_on_secret: i64,
    wh_on_open: i64,
}

async fn build_app() -> Seeded {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    // Pool size 1: this is a login+write test; a single warmed connection avoids
    // the SQLite cold-start (error 14) race that flakes such tests under nextest.
    let catalog = Catalog::connect_with_pool_size(&cat_uri, 1).await.unwrap();
    catalog.migrate().await.unwrap();
    let auth_db = AuthDb::connect_with_pool_size(&auth_uri, 1).await.unwrap();
    auth_db.migrate().await.unwrap();

    // `root` promoted to admin so its session scopes carry read/write:webhooks.
    let (root, _) = auth_db.ensure_principal("dummy", "root").await.unwrap();
    auth_db
        .update_principal_role(root.id, "admin")
        .await
        .unwrap();

    let secret = catalog
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: "secret".to_string(),
                structure_family: "container".to_string(),
                metadata: json!({}),
                specs: json!([]),
                access_blob: json!({}),
            },
        )
        .await
        .unwrap();
    let open = catalog
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: "open".to_string(),
                structure_family: "container".to_string(),
                metadata: json!({}),
                specs: json!([]),
                access_blob: json!({}),
            },
        )
        .await
        .unwrap();

    let wh_on_secret = catalog
        .create_webhook(WebhookCreate {
            node_id: secret.id,
            url: "https://example.com/secret-hook".to_string(),
            secret: None,
            events: None,
        })
        .await
        .unwrap()
        .id;
    let wh_on_open = catalog
        .create_webhook(WebhookCreate {
            node_id: open.id,
            url: "https://example.com/open-hook".to_string(),
            secret: None,
            events: None,
        })
        .await
        .unwrap()
        .id;

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

    let access_policy: Arc<dyn AccessPolicy> = Arc::new(SubtreeDenyPolicy {
        denied: "secret".to_string(),
    });

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
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    };
    Seeded {
        app: tiled_rs::server::build_app(state),
        wh_on_secret,
        wh_on_open,
    }
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
    body: Option<serde_json::Value>,
) -> StatusCode {
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", bearer);
    let body = match body {
        Some(v) => {
            req = req.header("content-type", "application/json");
            Body::from(serde_json::to_vec(&v).unwrap())
        }
        None => Body::empty(),
    };
    let resp = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
    resp.status()
}

fn reg_body() -> serde_json::Value {
    json!({"url": "https://example.com/new-hook"})
}

// --- Deny cases: the admin is node-restricted from `secret` by the policy. ---
// Each FAILS on the current (pre-fix) tree, where the routes skip the narrow.

#[tokio::test]
async fn webhook_register_denied_on_access_restricted_node() {
    let s = build_app().await;
    let bearer = login(&s.app).await;
    let status = send(
        &s.app,
        Method::POST,
        "/api/v1/webhooks/target/secret",
        &bearer,
        Some(reg_body()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "register on a policy-hidden node must be refused, got {status}"
    );
}

#[tokio::test]
async fn webhook_list_denied_on_access_restricted_node() {
    let s = build_app().await;
    let bearer = login(&s.app).await;
    let status = send(
        &s.app,
        Method::GET,
        "/api/v1/webhooks/target/secret",
        &bearer,
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "listing webhooks on a policy-hidden node must be refused, got {status}"
    );
}

#[tokio::test]
async fn webhook_delete_denied_on_access_restricted_node() {
    let s = build_app().await;
    let bearer = login(&s.app).await;
    let status = send(
        &s.app,
        Method::DELETE,
        &format!("/api/v1/webhooks/{}", s.wh_on_secret),
        &bearer,
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "deleting a webhook on a policy-hidden node must be refused, got {status}"
    );
}

#[tokio::test]
async fn webhook_history_denied_on_access_restricted_node() {
    let s = build_app().await;
    let bearer = login(&s.app).await;
    let status = send(
        &s.app,
        Method::GET,
        &format!("/api/v1/webhooks/history/{}", s.wh_on_secret),
        &bearer,
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "reading history of a webhook on a policy-hidden node must be refused, got {status}"
    );
}

// --- Positive control: the same admin is NOT restricted from `open`. These
// stay 200 both pre- and post-fix, proving the gate is a per-node narrow. ---

#[tokio::test]
async fn webhook_register_allowed_on_unrestricted_node() {
    let s = build_app().await;
    let bearer = login(&s.app).await;
    let status = send(
        &s.app,
        Method::POST,
        "/api/v1/webhooks/target/open",
        &bearer,
        Some(reg_body()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "register on an unrestricted node must still succeed, got {status}"
    );
}

#[tokio::test]
async fn webhook_history_allowed_on_unrestricted_node() {
    let s = build_app().await;
    let bearer = login(&s.app).await;
    let status = send(
        &s.app,
        Method::GET,
        &format!("/api/v1/webhooks/history/{}", s.wh_on_open),
        &bearer,
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "history on an unrestricted node must still succeed, got {status}"
    );
}
