//! F4: an EXPLICIT `?field=` projection naming an access-hidden child is
//! rejected IDENTICALLY to one naming a truly-absent child — same status, same
//! body (400 `No such field {key}.`) — so the response cannot be used to
//! enumerate hidden children; an unfiltered LISTING still silently drops them.
//! (Supersedes the interim hidden→404, which left hidden ≠ absent and leaked
//! presence via the 404-vs-400 distinction.)
//!
//! Exercises the `application/json` path end-to-end with a catalog +
//! TagBasedPolicy — the only backend that produces a per-child visible/hidden
//! split (in-memory nodes carry no access_blob, so their filter is
//! all-or-nothing). All `/container/full` formats route their projection
//! through the single `apply_child_projection` owner, so this one path pins the
//! shared rule.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::access::{ScopeSet, TagBasedPolicy};
use tiled_rs::auth::{AuthDb, DummyAuthenticator, Issuer};
use tiled_rs::catalog::{Catalog, node::RegisterRequest};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

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

async fn get(app: &axum::Router, uri: &str, bearer: &str) -> (StatusCode, Vec<u8>) {
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header("authorization", bearer)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

/// The child `id`s of an `application/json-seq` listing, in emitted order. Each
/// RS(0x1E)-framed record is one `Resource`. (The json-seq/html listing branch,
/// unlike the `application/json` tree, does not skip empty containers.)
fn json_seq_ids(bytes: &[u8]) -> Vec<String> {
    let mut ids: Vec<String> = bytes
        .split(|b| *b == 0x1E)
        .filter(|chunk| chunk.iter().any(|b| !b.is_ascii_whitespace()))
        .map(|chunk| {
            let v: serde_json::Value = serde_json::from_slice(chunk).unwrap();
            v["id"].as_str().unwrap().to_string()
        })
        .collect();
    ids.sort();
    ids
}

#[tokio::test]
async fn explicit_hidden_field_matches_absent_but_listing_filters_silently() {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    let catalog = Catalog::connect(&cat_uri).await.unwrap();
    catalog.migrate().await.unwrap();
    let auth_db = AuthDb::connect(&auth_uri).await.unwrap();
    auth_db.migrate().await.unwrap();

    // alice is a plain "user" (has read:data + read:metadata, NOT admin — so the
    // row-level filter applies) with the "team-a" tag only.
    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();
    auth_db
        .set_principal_tags(alice.id, &["team-a".to_string()])
        .await
        .unwrap();

    let node = |key: &str, access_blob: serde_json::Value| RegisterRequest {
        key: key.to_string(),
        structure_family: "container".to_string(),
        metadata: json!({}),
        specs: json!([]),
        access_blob,
    };
    // A public container `ds` with a visible child `vis` (untagged) and an
    // access-hidden child `hid` (tagged team-b, which alice cannot see).
    let ds = catalog
        .create_node(None, vec![], node("ds", json!({})))
        .await
        .unwrap();
    catalog
        .create_node(Some(ds.id), vec!["ds".into()], node("vis", json!({})))
        .await
        .unwrap();
    catalog
        .create_node(
            Some(ds.id),
            vec!["ds".into()],
            node("hid", json!({"tags": ["team-b"]})),
        )
        .await
        .unwrap();
    // A sibling container `ds2` with NO `hid` child at all — the "truly absent"
    // reference point. `?field=hid` on `ds2` is a genuinely-missing field; its
    // response must be byte-identical to `?field=hid` on `ds` (where `hid` is
    // merely access-hidden), so the two cases are indistinguishable.
    let ds2 = catalog
        .create_node(None, vec![], node("ds2", json!({})))
        .await
        .unwrap();
    catalog
        .create_node(Some(ds2.id), vec!["ds2".into()], node("vis2", json!({})))
        .await
        .unwrap();

    let policy = TagBasedPolicy::new(Arc::new(auth_db.clone()), ScopeSet::full());
    let access_policy: Arc<dyn tiled_rs::access::AccessPolicy> = Arc::new(policy);
    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> =
        Arc::new(tiled_rs::catalog::adapter::UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let mut dummy = DummyAuthenticator::new("dummy");
    dummy.add_user("alice", "wonderland").unwrap();

    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: Arc::new(tiled_rs::serialization::default_registry()),
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
        default_login_scopes: ScopeSet::full(),
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
        validation: Default::default(),
    };
    let app = tiled_rs::server::build_app(state);
    let token = login(&app, "alice", "wonderland").await;
    let bearer = format!("Bearer {token}");

    // Existence-hiding: an explicit `?field=hid` on `ds` (where `hid` is
    // access-hidden) must be BYTE-IDENTICAL to `?field=hid` on `ds2` (where `hid`
    // is genuinely absent) — same status, same body — so the response is not a
    // presence oracle. Probing the SAME name in both keeps the echoed field name
    // identical, so any residual difference would be a real leak. The absent case
    // keeps upstream parity (KeyError → 400 "No such field hid."), so the hidden
    // case returns that SAME 400 — NOT a silent drop and NOT a distinguishable 404.
    let (hidden_status, hidden_body) = get(
        &app,
        "/api/v1/container/full/ds?format=application/json-seq&field=hid",
        &bearer,
    )
    .await;
    let (absent_status, absent_body) = get(
        &app,
        "/api/v1/container/full/ds2?format=application/json-seq&field=hid",
        &bearer,
    )
    .await;
    assert_eq!(
        hidden_status,
        StatusCode::BAD_REQUEST,
        "an access-hidden field must 400, matching a truly-absent one"
    );
    assert_eq!(
        (hidden_status, &hidden_body),
        (absent_status, &absent_body),
        "access-hidden `hid` on ds must be byte-identical to absent `hid` on ds2"
    );

    // Uniform across output formats (single apply_child_projection owner): the
    // application/json tree path also collapses hidden ≡ absent to the same 400.
    let (hidden_status, hidden_body) = get(
        &app,
        "/api/v1/container/full/ds?format=application/json&field=hid",
        &bearer,
    )
    .await;
    let (absent_status, absent_body) = get(
        &app,
        "/api/v1/container/full/ds2?format=application/json&field=hid",
        &bearer,
    )
    .await;
    assert_eq!(
        hidden_status,
        StatusCode::BAD_REQUEST,
        "the json-tree path must 400 on the hidden field"
    );
    assert_eq!(
        (hidden_status, &hidden_body),
        (absent_status, &absent_body),
        "the json-tree path must make hidden byte-identical to absent"
    );

    // Explicit ?field=vis (visible) → 200 with just vis.
    let (status, body) = get(
        &app,
        "/api/v1/container/full/ds?format=application/json-seq&field=vis",
        &bearer,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_seq_ids(&body), vec!["vis".to_string()]);

    // A listing WITHOUT explicit field selection still silently filters `hid`.
    let (status, body) = get(
        &app,
        "/api/v1/container/full/ds?format=application/json-seq",
        &bearer,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_seq_ids(&body),
        vec!["vis".to_string()],
        "a listing silently drops the hidden child"
    );
}
