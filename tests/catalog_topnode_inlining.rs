//! Catalog TOP-NODE `structure.contents` inlining on `GET /metadata/<node>`.
//!
//! Wave-34 (`#133`) ported the recursive `structure.contents` inlining gate
//! (`tiled/server/core.py:511-556`) into the in-memory metadata path
//! (`construct_resource` / `build_container_structure`) and the search path
//! (`construct_entries_response`). But a **catalog-registered** container read
//! through `GET /metadata/<node>` was still served by `catalog_metadata_resource`
//! (`src/server/router.rs`), which hand-builds `NodeStructure { contents: None }`
//! from a child count and never routes an inline-enabled container through the
//! shared inlining owner. Upstream has NO such asymmetry — `/metadata` inlines
//! the addressed (top) node under the same `core.py:513` gate.
//!
//! The asymmetry these tests pin down: an `xarray_dataset`-tagged container
//! served from the SQL catalog returns `structure.contents == null`, whereas the
//! byte-equivalent in-memory node inlines its children (proven by
//! `tests/max_depth_inlining.rs::metadata_max_depth_none_inlines_recursively`).
//! After the fix the catalog top node inlines under the identical gate
//! (`max_depth`, the 500-child cap, child-Resource shape), while a plain
//! (unspec'd) container keeps the count-only fast path and never resolves a
//! child adapter.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use tiled_rs::access::{ScopeSet, TagBasedPolicy};
use tiled_rs::auth::{AuthDb, DummyAuthenticator, Issuer};
use tiled_rs::catalog::adapter::UnresolvedLeaf;
use tiled_rs::catalog::{Catalog, CatalogAdapter, RegisterRequest};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

/// Register one node under `parent_id` and return its DB id. `specs` is the raw
/// stored JSON array (`[]` = plain container, `[{"name":"xarray_dataset"}]` =
/// inline-enabled).
async fn create(
    catalog: &Catalog,
    parent_id: Option<i64>,
    ancestors: Vec<String>,
    key: &str,
    structure_family: &str,
    specs: serde_json::Value,
) -> i64 {
    catalog
        .create_node(
            parent_id,
            ancestors,
            RegisterRequest {
                key: key.into(),
                structure_family: structure_family.into(),
                metadata: serde_json::json!({}),
                specs,
                access_blob: serde_json::json!({}),
            },
        )
        .await
        .expect("create_node")
        .id
}

/// Catalog-backed app with the fixture tree:
/// ```text
/// root
/// ├── ds        (container, spec "xarray_dataset")  → inline-enabled
/// │   ├── x     (empty container)
/// │   └── y     (empty container)
/// ├── plain     (container, no spec)                → count-only fast path
/// │   └── a     (empty container)
/// └── plainleaf (container, no spec)                → count-only fast path
///     └── arr   (array leaf, UnresolvedLeaf)         → must NOT be resolved
/// ```
/// The `UnresolvedLeaf` resolver errors on any leaf resolution, so `plainleaf`
/// staying 200 (contents=null) proves the fast path builds no child adapter.
/// `ds`'s children are containers, resolvable without a leaf resolver.
async fn build_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let ds = create(
        &catalog,
        None,
        vec![],
        "ds",
        "container",
        serde_json::json!([{"name": "xarray_dataset"}]),
    )
    .await;
    create(
        &catalog,
        Some(ds),
        vec!["ds".into()],
        "x",
        "container",
        serde_json::json!([]),
    )
    .await;
    create(
        &catalog,
        Some(ds),
        vec!["ds".into()],
        "y",
        "container",
        serde_json::json!([]),
    )
    .await;

    let plain = create(
        &catalog,
        None,
        vec![],
        "plain",
        "container",
        serde_json::json!([]),
    )
    .await;
    create(
        &catalog,
        Some(plain),
        vec!["plain".into()],
        "a",
        "container",
        serde_json::json!([]),
    )
    .await;

    let plainleaf = create(
        &catalog,
        None,
        vec![],
        "plainleaf",
        "container",
        serde_json::json!([]),
    )
    .await;
    create(
        &catalog,
        Some(plainleaf),
        vec!["plainleaf".into()],
        "arr",
        "array",
        serde_json::json!([]),
    )
    .await;

    (plain_app_from(catalog), dir)
}

/// Build a catalog-backed app with NO access policy from an already-populated
/// `catalog`. Factored out of [`build_app`] so a fixture with a different tree
/// (empty eligible container, nested recursion, the 500-child cap) reuses the
/// identical policy-free state instead of duplicating the `AppState` literal.
fn plain_app_from(catalog: Catalog) -> axum::Router {
    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> =
        Arc::new(CatalogAdapter::root(catalog.clone(), resolver));
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
    tiled_rs::server::build_app(state)
}

async fn get_json(app: &axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
}

/// RED pre-fix: an `xarray_dataset` catalog container inlines its children into
/// `structure.contents` on `GET /metadata/<node>`, exactly as the in-memory node
/// does. Pre-fix `catalog_metadata_resource` returns `contents: null`.
#[tokio::test]
async fn catalog_xarray_dataset_topnode_inlines() {
    let (app, _dir) = build_app().await;
    let (status, body) = get_json(&app, "/api/v1/metadata/ds").await;
    assert_eq!(status, StatusCode::OK, "metadata/ds must be 200: {body}");
    let s = &body["data"]["attributes"]["structure"];
    assert_eq!(s["count"], 2, "ds has two children");
    assert!(
        s["contents"].is_object(),
        "xarray_dataset top node must inline its children (got contents={}): {s}",
        s["contents"]
    );
    let contents = s["contents"].as_object().unwrap();
    assert!(
        contents.contains_key("x") && contents.contains_key("y"),
        "both children inlined: {s}"
    );
    // Child-Resource shape parity with the in-memory `construct_resource` path.
    assert_eq!(s["contents"]["x"]["id"], "x");
    assert_eq!(
        s["contents"]["x"]["attributes"]["structure_family"],
        "container"
    );
    // `x` is an empty plain container (depth 1): its own contents stay null,
    // count 0 — the recursion respects the per-node gate.
    assert!(
        s["contents"]["x"]["attributes"]["structure"]["contents"].is_null(),
        "empty plain child must not inline: {s}"
    );
    assert_eq!(s["contents"]["x"]["attributes"]["structure"]["count"], 0);
}

/// A plain (unspec'd) catalog container keeps `contents: null` — the count-only
/// fast path — both before and after the fix.
#[tokio::test]
async fn catalog_plain_container_contents_null() {
    let (app, _dir) = build_app().await;
    let (status, body) = get_json(&app, "/api/v1/metadata/plain").await;
    assert_eq!(status, StatusCode::OK);
    let s = &body["data"]["attributes"]["structure"];
    assert!(
        s["contents"].is_null(),
        "plain container must not inline: {s}"
    );
    assert_eq!(s["count"], 1, "plain container count");
}

/// `?max_depth=0` disables inlining even on an inline-enabled catalog node
/// (`0 < 0` is false), mirroring the in-memory gate.
#[tokio::test]
async fn catalog_topnode_max_depth_zero_no_inline() {
    let (app, _dir) = build_app().await;
    let (status, body) = get_json(&app, "/api/v1/metadata/ds?max_depth=0").await;
    assert_eq!(status, StatusCode::OK);
    let s = &body["data"]["attributes"]["structure"];
    assert!(s["contents"].is_null(), "max_depth=0 must not inline: {s}");
    assert_eq!(s["count"], 2, "count still reported");
}

/// The fast path builds no child adapters: `plainleaf` holds an `UnresolvedLeaf`
/// array child that errors on resolution. A count-only response stays 200 with
/// `contents: null`; had the request resolved the child it would surface the
/// resolver error instead. Guards the "plain containers pay nothing new"
/// invariant observably.
#[tokio::test]
async fn catalog_plain_container_fast_path_no_child_build() {
    let (app, _dir) = build_app().await;
    let (status, body) = get_json(&app, "/api/v1/metadata/plainleaf").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "fast path must not resolve the unresolvable leaf child: {body}"
    );
    let s = &body["data"]["attributes"]["structure"];
    assert!(
        s["contents"].is_null(),
        "plain container stays non-inlined: {s}"
    );
    assert_eq!(s["count"], 1);
}

/// `?omit_links=true` shaping reaches the INLINED children, not just the
/// addressed node: `build_container_structure` shapes each inlined child with the
/// same `ShapeOptions` the metadata handler computed (Wave-35 Finding 2). A
/// `Resource` drops its `links` key entirely when empty (`schemas.rs` —
/// `skip_serializing_if = "NodeLinks::is_empty"`), so a shaped child has NO
/// `links` key. Baseline (no `omit_links`) keeps the child's `self` link, which
/// isolates the shaping to the flag.
#[tokio::test]
async fn catalog_topnode_inline_children_shaped_by_omit_links() {
    let (app, _dir) = build_app().await;

    // Baseline: without omit_links, the inlined child carries its own links.
    let (status, body) = get_json(&app, "/api/v1/metadata/ds").await;
    assert_eq!(status, StatusCode::OK, "metadata/ds must be 200: {body}");
    let child = &body["data"]["attributes"]["structure"]["contents"]["x"];
    assert!(
        child["links"]["self"].is_string(),
        "baseline inlined child keeps its self link: {child}"
    );

    // With omit_links=true, the handler shapes the addressed node AND the shape
    // is threaded into the inline walk, so the inlined child is shaped too: both
    // drop the `links` key.
    let (status, body) = get_json(&app, "/api/v1/metadata/ds?omit_links=true").await;
    assert_eq!(status, StatusCode::OK, "metadata/ds must be 200: {body}");
    let data = &body["data"];
    assert!(
        data.get("links").is_none(),
        "omit_links drops the addressed node's links: {data}"
    );
    let child = &data["attributes"]["structure"]["contents"]["x"];
    assert!(
        child.is_object() && child.get("links").is_none(),
        "omit_links must reach the inlined child (shape threaded to \
         build_container_structure): {child}"
    );
}

/// Register one node under `parent_id` with an explicit `access_blob`, returning
/// its DB id. Distinct from [`create`] (which hardcodes an empty access blob) so
/// the access-filter fixture can tag a child.
async fn create_with_access(
    catalog: &Catalog,
    parent_id: Option<i64>,
    ancestors: Vec<String>,
    key: &str,
    specs: serde_json::Value,
    access_blob: serde_json::Value,
) -> i64 {
    catalog
        .create_node(
            parent_id,
            ancestors,
            RegisterRequest {
                key: key.into(),
                structure_family: "container".into(),
                metadata: serde_json::json!({}),
                specs,
                access_blob,
            },
        )
        .await
        .expect("create_node")
        .id
}

/// Catalog-backed app under a `TagBasedPolicy`, with a dummy authenticator so a
/// tagged principal can log in. Fixture tree (mirrors
/// `tests/inline_access_filter.rs`, but exercised via the `/metadata/<node>`
/// top-node path rather than `/search`):
/// ```text
/// ds         (container, spec "xarray_dataset", "public") → inline-enabled
/// ├── visible   (container, tagged "public")             → readable by all
/// └── secret    (container, tagged "team-b")              → hidden from team-a
/// pc         (plain container, "public")                  → count-only fast path
/// ├── pvis      (container, tagged "public")             → readable by all
/// └── phid      (container, tagged "team-b")              → hidden from team-a
/// roothidden (plain container, tagged "team-b")           → hidden root child
/// ```
/// Alice is granted `team-a` only, so at the root she sees `ds` + `pc` (2 of 3),
/// inside `pc` she sees `pvis` (1 of 2), and inside `ds` she sees `visible`
/// (1 of 2).
async fn build_access_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    // Size-1 warm pools sidestep the SQLite cold-start CANTOPEN flake on
    // login+write integration tests.
    let catalog = Catalog::connect_with_pool_size(&cat_uri, 1).await.unwrap();
    catalog.migrate().await.unwrap();
    let auth_db = AuthDb::connect_with_pool_size(&auth_uri, 1).await.unwrap();
    auth_db.migrate().await.unwrap();

    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();
    auth_db
        .set_principal_tags(alice.id, &["team-a".to_string()])
        .await
        .unwrap();

    // ds (xarray_dataset, "public") → { visible ("public"), secret (team-b) }
    let ds = create_with_access(
        &catalog,
        None,
        vec![],
        "ds",
        serde_json::json!(["xarray_dataset"]),
        serde_json::json!({"tags": ["public"]}),
    )
    .await;
    create_with_access(
        &catalog,
        Some(ds),
        vec!["ds".into()],
        "visible",
        serde_json::json!([]),
        serde_json::json!({"tags": ["public"]}),
    )
    .await;
    create_with_access(
        &catalog,
        Some(ds),
        vec!["ds".into()],
        "secret",
        serde_json::json!([]),
        serde_json::json!({"tags": ["team-b"]}),
    )
    .await;

    // pc (plain container, "public") → { pvis ("public"), phid (team-b) }. A
    // plain container takes the count-only fast path (no inlining), so its count
    // MUST also be principal-scoped.
    let pc = create_with_access(
        &catalog,
        None,
        vec![],
        "pc",
        serde_json::json!([]),
        serde_json::json!({"tags": ["public"]}),
    )
    .await;
    create_with_access(
        &catalog,
        Some(pc),
        vec!["pc".into()],
        "pvis",
        serde_json::json!([]),
        serde_json::json!({"tags": ["public"]}),
    )
    .await;
    create_with_access(
        &catalog,
        Some(pc),
        vec!["pc".into()],
        "phid",
        serde_json::json!([]),
        serde_json::json!({"tags": ["team-b"]}),
    )
    .await;

    // roothidden (plain container, team-b) → hidden from alice at the root, so
    // the root count-only fast path must report 2 (ds + pc), not 3.
    create_with_access(
        &catalog,
        None,
        vec![],
        "roothidden",
        serde_json::json!([]),
        serde_json::json!({"tags": ["team-b"]}),
    )
    .await;

    (access_app_from(catalog, auth_db), dir)
}

/// Build a catalog-backed app under a `TagBasedPolicy` from an already-populated
/// `catalog` + `auth_db`. Factored out of [`build_access_app`] so a fixture with a
/// different tree (e.g. the >500-child boundary) reuses the identical state.
fn access_app_from(catalog: Catalog, auth_db: AuthDb) -> axum::Router {
    let policy = TagBasedPolicy::new(Arc::new(auth_db.clone()), ScopeSet::full());
    let access_policy: Arc<dyn tiled_rs::access::AccessPolicy> = Arc::new(policy);
    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> =
        Arc::new(CatalogAdapter::root(catalog.clone(), resolver));
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
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
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
    tiled_rs::server::build_app(state)
}

async fn get_json_auth(
    app: &axum::Router,
    uri: &str,
    bearer: &str,
) -> (StatusCode, serde_json::Value) {
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header("authorization", bearer)
        .header("accept", "application/json")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
}

async fn login(app: &axum::Router, username: &str, password: &str) -> String {
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/dummy/login")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({"username": username, "password": password}))
                .unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    body["access_token"].as_str().unwrap().to_string()
}

/// Access-filter closure for the top-node path (Wave-35 Finding 1): the inline
/// walk on `GET /metadata/<node>` MUST route child enumeration through the
/// caller's `list_filter`, never raw `keys()`. Alice (team-a) reads the
/// `xarray_dataset` top node `ds`; its `visible` child inlines, its `secret`
/// (team-b) child MUST be absent from `structure.contents`. `count` is
/// principal-scoped: it reports only the visible child (1), NOT the full
/// cardinality — matching `tests/inline_access_filter.rs` on the `/search` path
/// and upstream `len_or_approx` over the `filter_for_access` view (core.py:509).
/// A direct GET of the hidden child 404s, proving the inline path cannot surface
/// what a direct read denies.
#[tokio::test]
async fn catalog_topnode_inline_walk_routes_through_access_filter() {
    let (app, _dir) = build_access_app().await;
    let token = login(&app, "alice", "wonderland").await;
    let bearer = format!("Bearer {token}");

    let (status, body) = get_json_auth(&app, "/api/v1/metadata/ds", &bearer).await;
    assert_eq!(status, StatusCode::OK, "metadata/ds must be 200: {body}");
    let s = &body["data"]["attributes"]["structure"];
    let contents = &s["contents"];
    assert!(
        contents.is_object(),
        "ds (xarray_dataset) top node must inline its children: {s}"
    );
    assert!(
        contents.get("visible").is_some(),
        "the visible child must be inlined: {contents}"
    );
    assert!(
        contents.get("secret").is_none(),
        "ACCESS LEAK: the access-filtered child `secret` must NOT be inlined \
         into the top node's contents: {contents}"
    );
    // Count is principal-scoped: only the visible child is counted, so `count`
    // equals the number of `contents` entries (1). The access-filtered `secret`
    // is absent from `contents` AND uncounted.
    assert_eq!(
        s["count"], 1,
        "count is the caller-visible child count (secret is hidden AND uncounted): {s}"
    );

    // Consistency: a direct GET of the hidden child 404s (read:metadata denied),
    // so the inline path must not surface it either.
    let (secret_status, _) = get_json_auth(&app, "/api/v1/metadata/ds/secret", &bearer).await;
    assert_eq!(
        secret_status,
        StatusCode::NOT_FOUND,
        "direct GET of the access-hidden child must 404"
    );
}

/// Count-only fast path (a plain, non-`xarray_dataset` container) must ALSO be
/// principal-scoped. `pc` has `pvis` (visible) + `phid` (team-b, hidden); alice
/// sees 1 of 2, so `GET /metadata/pc` must report `count == 1`, matching what a
/// `/search/pc` listing (already filtered) reports as `meta.count`.
#[tokio::test]
async fn catalog_topnode_plain_container_count_is_principal_scoped() {
    let (app, _dir) = build_access_app().await;
    let token = login(&app, "alice", "wonderland").await;
    let bearer = format!("Bearer {token}");

    let (status, body) = get_json_auth(&app, "/api/v1/metadata/pc", &bearer).await;
    assert_eq!(status, StatusCode::OK, "metadata/pc must be 200: {body}");
    let s = &body["data"]["attributes"]["structure"];
    // Plain container → count-only fast path, no inlining.
    assert!(
        s["contents"].is_null(),
        "a plain container keeps contents:null: {s}"
    );
    assert_eq!(
        s["count"], 1,
        "plain-container count is the caller-visible child count (phid is hidden AND uncounted): {s}"
    );

    // Consistency: a `/search/pc` listing reports the same filtered count.
    let (sstatus, sbody) = get_json_auth(
        &app,
        "/api/v1/search/pc?page[offset]=0&page[limit]=100",
        &bearer,
    )
    .await;
    assert_eq!(sstatus, StatusCode::OK, "search/pc must be 200: {sbody}");
    assert_eq!(
        sbody["meta"]["count"], 1,
        "search meta.count and metadata count must agree for the same container: {sbody}"
    );
}

/// The root container's count-only fast path must be principal-scoped too. The
/// root holds `ds` + `pc` (visible) + `roothidden` (team-b, hidden); alice sees
/// 2 of 3, so `GET /metadata/` must report `count == 2`.
#[tokio::test]
async fn catalog_root_count_is_principal_scoped() {
    let (app, _dir) = build_access_app().await;
    let token = login(&app, "alice", "wonderland").await;
    let bearer = format!("Bearer {token}");

    let (status, body) = get_json_auth(&app, "/api/v1/metadata/", &bearer).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "metadata/ (root) must be 200: {body}"
    );
    let s = &body["data"]["attributes"]["structure"];
    assert_eq!(
        s["count"], 2,
        "root count is the caller-visible child count (roothidden is hidden AND uncounted): {s}"
    );
}

/// Inline gate boundary: a container with MORE than `INLINED_CONTENTS_LIMIT`
/// (500) total children but at most 500 *visible* to the caller must inline the
/// visible set — the gate and cap operate on the permitted (filtered) count, not
/// the full cardinality. Here `big` (xarray_dataset) has 500 hidden (team-b)
/// children plus 2 visible, so total = 502 > 500 while alice sees only 2.
/// Pre-fix, the full count (502) exceeded the cap and inlining was suppressed
/// (`contents: null`, `count: 502`); post-fix the visible 2 inline (`count: 2`).
#[tokio::test]
async fn catalog_topnode_inline_gate_uses_visible_count() {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    let catalog = Catalog::connect_with_pool_size(&cat_uri, 1).await.unwrap();
    catalog.migrate().await.unwrap();
    let auth_db = AuthDb::connect_with_pool_size(&auth_uri, 1).await.unwrap();
    auth_db.migrate().await.unwrap();

    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();
    auth_db
        .set_principal_tags(alice.id, &["team-a".to_string()])
        .await
        .unwrap();

    let big = create_with_access(
        &catalog,
        None,
        vec![],
        "big",
        serde_json::json!(["xarray_dataset"]),
        serde_json::json!({"tags": ["public"]}),
    )
    .await;
    // 500 hidden (team-b) children push the FULL count past the 500 cap.
    for i in 0..500 {
        create_with_access(
            &catalog,
            Some(big),
            vec!["big".into()],
            &format!("hidden{i:04}"),
            serde_json::json!([]),
            serde_json::json!({"tags": ["team-b"]}),
        )
        .await;
    }
    // 2 visible (public-tagged) children — the only ones alice may see.
    for key in ["vis_a", "vis_b"] {
        create_with_access(
            &catalog,
            Some(big),
            vec!["big".into()],
            key,
            serde_json::json!([]),
            serde_json::json!({"tags": ["public"]}),
        )
        .await;
    }

    let app = access_app_from(catalog, auth_db);
    let token = login(&app, "alice", "wonderland").await;
    let bearer = format!("Bearer {token}");

    let (status, body) = get_json_auth(&app, "/api/v1/metadata/big", &bearer).await;
    assert_eq!(status, StatusCode::OK, "metadata/big must be 200: {body}");
    let s = &body["data"]["attributes"]["structure"];
    let contents = &s["contents"];
    assert!(
        contents.is_object(),
        "502 total but 2 visible (<= 500 cap): the visible set MUST inline: {s}"
    );
    assert!(
        contents.get("vis_a").is_some() && contents.get("vis_b").is_some(),
        "both visible children must be inlined: {contents}"
    );
    assert!(
        contents.get("hidden0000").is_none(),
        "ACCESS LEAK: no hidden child may be inlined: {contents}"
    );
    assert_eq!(
        s["count"], 2,
        "count is the visible child count (2), not the full 502: {s}"
    );
}

// ===========================================================================
// Invariant-boundary regression tests (PR #139 review coverage gaps), written
// against the post-#141 principal-scoped count semantics and the post-#142
// always-serialized `contents` (explicit `null` when not inlined).
// ===========================================================================

/// Boundary 1 — an eligible-but-EMPTY container.
///
/// An `xarray_dataset` container with zero children still passes the inline gate
/// (`inlined_contents_enabled` is spec-driven, count `0 <= 500`), so it inlines an
/// EMPTY object: `contents == {}`, `count == 0`. This is structurally distinct
/// from a plain (unspec'd) container, whose count-only fast path emits
/// `contents == null`. The empty-object vs `null` distinction is the whole point:
/// `{}` means "inlined, and there is nothing", `null` means "not inlined".
#[tokio::test]
async fn catalog_empty_eligible_container_inlines_empty_object() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    // Eligible but childless.
    create(
        &catalog,
        None,
        vec![],
        "empty_ds",
        "container",
        serde_json::json!([{"name": "xarray_dataset"}]),
    )
    .await;
    // Plain and childless — the contrast partner.
    create(
        &catalog,
        None,
        vec![],
        "empty_plain",
        "container",
        serde_json::json!([]),
    )
    .await;
    let app = plain_app_from(catalog);

    let (status, body) = get_json(&app, "/api/v1/metadata/empty_ds").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "metadata/empty_ds must be 200: {body}"
    );
    let s_ds = &body["data"]["attributes"]["structure"];
    assert!(
        s_ds["contents"].is_object(),
        "an empty eligible container inlines an object, not null: {s_ds}"
    );
    assert_eq!(
        s_ds["contents"].as_object().unwrap().len(),
        0,
        "the inlined object is empty (no children): {s_ds}"
    );
    assert_eq!(s_ds["count"], 0, "empty container count is 0: {s_ds}");

    let (status, body) = get_json(&app, "/api/v1/metadata/empty_plain").await;
    assert_eq!(status, StatusCode::OK);
    let s_plain = &body["data"]["attributes"]["structure"];
    assert!(
        s_plain["contents"].is_null(),
        "a plain empty container keeps contents:null (count-only fast path): {s_plain}"
    );
    assert_eq!(s_plain["count"], 0, "plain empty container count is 0");

    // The boundary: {} (inlined-and-empty) and null (not inlined) must not be
    // conflated even though both containers hold zero children.
    assert_ne!(
        s_ds["contents"], s_plain["contents"],
        "empty-eligible {{}} and plain-empty null must stay distinct: \
         ds={} plain={}",
        s_ds["contents"], s_plain["contents"]
    );
}

/// Boundary 2 — multi-level recursion and the `max_depth` gate.
///
/// Tree: `ds` (xarray_dataset) → `mid` (xarray_dataset) → `leaf` (plain, empty).
///
/// - `max_depth` absent (None) recurses to the `DEPTH_LIMIT` bound: `ds` inlines
///   `mid`, and `mid`'s OWN `structure.contents` inlines the grandchild `leaf`.
/// - `?max_depth=1` inlines only `ds`'s direct children: `mid` appears, but
///   `mid`'s own `structure.contents` is an EXPLICIT `null` KEY (per #142 —
///   present, value null), NOT recursed into. No grandchild leaks.
#[tokio::test]
async fn catalog_topnode_multilevel_recursion_respects_max_depth() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let ds = create(
        &catalog,
        None,
        vec![],
        "ds",
        "container",
        serde_json::json!([{"name": "xarray_dataset"}]),
    )
    .await;
    let mid = create(
        &catalog,
        Some(ds),
        vec!["ds".into()],
        "mid",
        "container",
        serde_json::json!([{"name": "xarray_dataset"}]),
    )
    .await;
    create(
        &catalog,
        Some(mid),
        vec!["ds".into(), "mid".into()],
        "leaf",
        "container",
        serde_json::json!([]),
    )
    .await;
    let app = plain_app_from(catalog);

    // max_depth=None → recurse into the nested eligible child; grandchild inlined.
    let (status, body) = get_json(&app, "/api/v1/metadata/ds").await;
    assert_eq!(status, StatusCode::OK, "metadata/ds must be 200: {body}");
    let s = &body["data"]["attributes"]["structure"];
    assert_eq!(s["count"], 1, "ds has one child (mid): {s}");
    let mid_struct = &s["contents"]["mid"]["attributes"]["structure"];
    assert_eq!(
        mid_struct["count"], 1,
        "mid has one child (leaf): {mid_struct}"
    );
    let grandchild = &mid_struct["contents"]["leaf"];
    assert!(
        grandchild.is_object(),
        "max_depth=None must recurse into the nested eligible child: the \
         grandchild `leaf` must be inlined under mid: {mid_struct}"
    );
    assert_eq!(
        grandchild["attributes"]["structure_family"], "container",
        "grandchild is a container: {grandchild}"
    );
    // The grandchild is itself a plain empty container: its own contents stay null.
    assert!(
        grandchild["attributes"]["structure"]["contents"].is_null(),
        "the plain grandchild does not inline further: {grandchild}"
    );

    // max_depth=1 → inline ds's children only; mid's own contents is EXPLICIT null.
    let (status, body) = get_json(&app, "/api/v1/metadata/ds?max_depth=1").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "metadata/ds?max_depth=1 must be 200: {body}"
    );
    let s1 = &body["data"]["attributes"]["structure"];
    assert_eq!(
        s1["count"], 1,
        "ds still has one child at max_depth=1: {s1}"
    );
    let mid_obj = s1["contents"]["mid"]["attributes"]["structure"]
        .as_object()
        .unwrap_or_else(|| panic!("mid structure must be an object: {s1}"));
    assert!(
        mid_obj.contains_key("contents"),
        "post-#142: the non-inlined nested child carries an EXPLICIT `contents` \
         key: {mid_obj:?}"
    );
    assert!(
        mid_obj["contents"].is_null(),
        "max_depth=1 stops one level shallower: mid's own contents is null, not \
         recursed: {mid_obj:?}"
    );
    assert_eq!(
        mid_obj["count"], 1,
        "the count is still reported on the non-inlined nested child: {mid_obj:?}"
    );
}

/// Boundary 3 — the `INLINED_CONTENTS_LIMIT` (500) cap on the CATALOG path.
///
/// The shared owner (`build_container_structure`) inlines when the visible count
/// is `<= 500` and suppresses (`contents: null`) when it is `> 500`; `count` is
/// always reported. This exercises that boundary through the catalog top-node
/// route (not the in-memory path `tests/max_depth_inlining.rs` already covers):
/// `cap500` (500 children) inlines all 500; `cap501` (501 children) does not
/// inline but still reports `count == 501`. No access policy here, so the visible
/// count equals the full count.
#[tokio::test]
async fn catalog_topnode_inline_cap_500_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let cap500 = create(
        &catalog,
        None,
        vec![],
        "cap500",
        "container",
        serde_json::json!([{"name": "xarray_dataset"}]),
    )
    .await;
    for i in 0..500 {
        create(
            &catalog,
            Some(cap500),
            vec!["cap500".into()],
            &format!("v{i:04}"),
            "container",
            serde_json::json!([]),
        )
        .await;
    }

    let cap501 = create(
        &catalog,
        None,
        vec![],
        "cap501",
        "container",
        serde_json::json!([{"name": "xarray_dataset"}]),
    )
    .await;
    for i in 0..501 {
        create(
            &catalog,
            Some(cap501),
            vec!["cap501".into()],
            &format!("v{i:04}"),
            "container",
            serde_json::json!([]),
        )
        .await;
    }
    let app = plain_app_from(catalog);

    // Exactly at the cap → inline all 500.
    let (status, body) = get_json(&app, "/api/v1/metadata/cap500").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "metadata/cap500 must be 200: {body}"
    );
    let s = &body["data"]["attributes"]["structure"];
    assert_eq!(
        s["contents"].as_object().map(|o| o.len()),
        Some(500),
        "exactly 500 children (== cap) must all inline: count={}",
        s["count"]
    );
    assert_eq!(s["count"], 500, "count at the cap boundary");

    // One over the cap → suppress inlining, but still report the count.
    let (status, body) = get_json(&app, "/api/v1/metadata/cap501").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "metadata/cap501 must be 200: {body}"
    );
    let s = &body["data"]["attributes"]["structure"];
    assert!(
        s["contents"].is_null(),
        "501 children (> cap) must NOT inline (contents:null): {}",
        s["contents"]
    );
    assert_eq!(
        s["count"], 501,
        "count is still reported when over the cap: {s}"
    );
}

/// Boundary 4 — a hidden child that is itself an ELIGIBLE container.
///
/// `ds` (xarray_dataset, "public") holds `vis` ("public", plain) and `hidden_ds`
/// (team-b — hidden from alice — AND itself `xarray_dataset` with an array-leaf
/// child). The danger the inline gate keys on the spec discriminator invites: an
/// eligible hidden child could be resolved and recursed. The access filter runs
/// FIRST, so `hidden_ds` must be: absent from `contents`, never resolved, never
/// recursed, and excluded from the principal-scoped `count`.
///
/// "Never resolved/recursed" is observable: `hidden_ds` carries an
/// `UnresolvedLeaf` array child that ERRORS on resolution. If the walk wrongly
/// recursed into `hidden_ds`, resolving that child would surface a 500 instead of
/// the 200 asserted here. A direct GET of the hidden child 404s, proving the
/// inline path cannot surface what a direct read denies.
#[tokio::test]
async fn catalog_topnode_hidden_eligible_child_never_recursed() {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    let catalog = Catalog::connect_with_pool_size(&cat_uri, 1).await.unwrap();
    catalog.migrate().await.unwrap();
    let auth_db = AuthDb::connect_with_pool_size(&auth_uri, 1).await.unwrap();
    auth_db.migrate().await.unwrap();

    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();
    auth_db
        .set_principal_tags(alice.id, &["team-a".to_string()])
        .await
        .unwrap();

    let ds = create_with_access(
        &catalog,
        None,
        vec![],
        "ds",
        serde_json::json!(["xarray_dataset"]),
        serde_json::json!({"tags": ["public"]}),
    )
    .await;
    create_with_access(
        &catalog,
        Some(ds),
        vec!["ds".into()],
        "vis",
        serde_json::json!([]),
        serde_json::json!({"tags": ["public"]}),
    )
    .await;
    // Hidden AND eligible: team-b spec xarray_dataset, with an array-leaf child
    // that the UnresolvedLeaf resolver refuses. Resolving/recursing it would 500.
    let hidden_ds = create_with_access(
        &catalog,
        Some(ds),
        vec!["ds".into()],
        "hidden_ds",
        serde_json::json!(["xarray_dataset"]),
        serde_json::json!({"tags": ["team-b"]}),
    )
    .await;
    // hidden_ds's array child (structure_family "array") → UnresolvedLeaf errors
    // if ever resolved.
    catalog
        .create_node(
            Some(hidden_ds),
            vec!["ds".into(), "hidden_ds".into()],
            RegisterRequest {
                key: "arr".into(),
                structure_family: "array".into(),
                metadata: serde_json::json!({}),
                specs: serde_json::json!([]),
                access_blob: serde_json::json!({}),
            },
        )
        .await
        .expect("create_node");

    let app = access_app_from(catalog, auth_db);
    let token = login(&app, "alice", "wonderland").await;
    let bearer = format!("Bearer {token}");

    let (status, body) = get_json_auth(&app, "/api/v1/metadata/ds", &bearer).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "metadata/ds must be 200 — a 500 would mean the walk resolved the hidden \
         eligible child's UnresolvedLeaf: {body}"
    );
    let s = &body["data"]["attributes"]["structure"];
    let contents = &s["contents"];
    assert!(contents.is_object(), "ds inlines its visible children: {s}");
    assert!(
        contents.get("vis").is_some(),
        "the visible child must be inlined: {contents}"
    );
    assert!(
        contents.get("hidden_ds").is_none(),
        "ACCESS LEAK: the hidden eligible child must NOT be inlined (never \
         resolved, never recursed): {contents}"
    );
    // No grandchild of the hidden subtree may appear anywhere in the object.
    assert!(
        contents.get("arr").is_none(),
        "the hidden child's own child must never leak into the top node: {contents}"
    );
    assert_eq!(
        s["count"], 1,
        "count is principal-scoped: only `vis` is visible, `hidden_ds` is hidden \
         AND uncounted: {s}"
    );

    // Consistency: a direct GET of the hidden child 404s (read denied), so the
    // inline path must not surface it either.
    let (hidden_status, _) = get_json_auth(&app, "/api/v1/metadata/ds/hidden_ds", &bearer).await;
    assert_eq!(
        hidden_status,
        StatusCode::NOT_FOUND,
        "direct GET of the access-hidden eligible child must 404"
    );
}
