//! Verify the `access_control:` config section wires a REAL per-node access
//! policy into the server — not the narrows-nothing PassthroughPolicy default.
//!
//! Mirrors crates/tiled-server/tests/access_filter_integration.rs, but the
//! TagBasedPolicy here is constructed from a parsed YAML config rather than
//! by hand, exercising the config -> construction -> injection path.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::auth::{AuthDb, DummyAuthenticator, Issuer};
use tiled_rs::catalog::{Catalog, node::RegisterRequest};
use tiled_rs::cli::config::TiledConfig;
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

async fn search_json(
    app: &axum::Router,
    auth_header: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/search/?page[offset]=0&page[limit]=100");
    if let Some(h) = auth_header {
        req = req.header("authorization", h);
    }
    let req = req
        .header("accept", "application/json")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
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
    body["access_token"].as_str().unwrap().to_string()
}

fn result_keys(body: &serde_json::Value) -> Vec<String> {
    body["data"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|e| e["id"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// A config selecting `tag_based` produces a server whose access_policy
/// narrows a forbidden node out of search results.
#[tokio::test]
async fn tag_based_config_narrows_forbidden_node() {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    let catalog = Catalog::connect(&cat_uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let auth_db = AuthDb::connect(&auth_uri).await.unwrap();
    auth_db.migrate().await.unwrap();

    // Pre-create alice so we know her UUID before writing the config.
    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();

    // Seed nodes with different access_blob tags.
    let make_node = |key: &str, access_blob: serde_json::Value| RegisterRequest {
        key: key.to_string(),
        structure_family: "container".to_string(),
        metadata: json!({}),
        specs: json!([]),
        access_blob,
    };
    catalog
        .create_node(None, vec![], make_node("public_node", json!({})))
        .await
        .unwrap();
    catalog
        .create_node(
            None,
            vec![],
            make_node("team_a_node", json!({"tags": ["team-a"]})),
        )
        .await
        .unwrap();
    catalog
        .create_node(
            None,
            vec![],
            make_node("team_b_node", json!({"tags": ["team-b"]})),
        )
        .await
        .unwrap();

    // Build the access policy FROM CONFIG: select tag_based, grant alice
    // "team-a" only. team_b_node must be narrowed out of her results.
    let yaml = format!(
        "access_control:\n  \
           access_policy: tag_based\n  \
           args:\n    \
             default_scopes: [read:metadata, read:data]\n    \
             grants:\n      \
               \"{uuid}\": [team-a]\n",
        uuid = alice.uuid
    );
    let config: TiledConfig = serde_yaml::from_str(&yaml).expect("config parses");
    let access_policy: Arc<dyn tiled_rs::access::AccessPolicy> = config
        .access_control
        .as_ref()
        .expect("access_control section present")
        .build()
        .await
        .expect("policy builds from config");

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
        access_policy: Some(access_policy),
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
    };
    let app = tiled_rs::server::build_app(state);

    let token = login(&app, "alice", "wonderland").await;
    let bearer = format!("Bearer {token}");

    let (status, body) = search_json(&app, Some(&bearer)).await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");
    let keys = result_keys(&body);
    assert!(
        keys.contains(&"public_node".to_string()),
        "alice must see public_node, got: {keys:?}"
    );
    assert!(
        keys.contains(&"team_a_node".to_string()),
        "alice must see team_a_node (granted), got: {keys:?}"
    );
    assert!(
        !keys.contains(&"team_b_node".to_string()),
        "config-built policy must narrow team_b_node out, got: {keys:?}"
    );
    assert_eq!(keys.len(), 2, "alice sees exactly 2 nodes, got: {keys:?}");
}

/// Unknown policy name → clear error.
#[tokio::test]
async fn unknown_policy_name_is_rejected() {
    let yaml = "access_control:\n  access_policy: bogus\n";
    let config: TiledConfig = serde_yaml::from_str(yaml).unwrap();
    let err = config
        .access_control
        .as_ref()
        .unwrap()
        .build()
        .await
        .err()
        .expect("unknown policy must error");
    let msg = err.to_string();
    assert!(msg.contains("unknown access_policy"), "got: {msg}");
    assert!(
        msg.contains("bogus"),
        "error must name the bad value: {msg}"
    );
}

/// `tag_based` without the required `grants` arg → clear error.
#[tokio::test]
async fn tag_based_missing_grants_is_rejected() {
    let yaml = "access_control:\n  access_policy: tag_based\n  args:\n    default_scopes: [read:metadata]\n";
    let config: TiledConfig = serde_yaml::from_str(yaml).unwrap();
    let err = config
        .access_control
        .as_ref()
        .unwrap()
        .build()
        .await
        .err()
        .expect("missing grants must error");
    let msg = err.to_string();
    assert!(msg.contains("grants"), "error must mention grants: {msg}");
}

/// `passthrough` (and `none`) build successfully and ignore args.
#[tokio::test]
async fn passthrough_and_none_build_successfully() {
    for name in ["passthrough", "none"] {
        let yaml = format!("access_control:\n  access_policy: {name}\n");
        let config: TiledConfig = serde_yaml::from_str(&yaml).unwrap();
        config
            .access_control
            .as_ref()
            .unwrap()
            .build()
            .await
            .unwrap_or_else(|e| panic!("{name} must build: {e}"));
    }
}
