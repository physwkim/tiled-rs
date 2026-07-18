//! Spec validation registry + `reject_undeclared_specs` (Finding 2) — boundary
//! tests over the HTTP write endpoints.
//!
//! Exercises the mechanism ported from upstream `tiled/validation_registration.py`
//! + `server/router.py::validate_specs`:
//!
//! * a registered validator runs on node CREATE and on metadata UPDATE
//!   (PATCH + PUT), rejecting or accepting per the metadata;
//! * an undeclared spec passes when `reject_undeclared_specs` is off and is a
//!   400 when it is on;
//! * a node carrying no specs is unaffected either way.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use tiled_rs::catalog::{Catalog, adapter::UnresolvedLeaf};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;
use tiled_rs::core::structures::Spec;
use tiled_rs::server::validation::{
    ValidationConfig, ValidationError, ValidationRegistry, Validator,
};

/// Validator for the `positive` spec: `metadata.value` must be a positive
/// number, else the node is rejected.
fn positive_validator() -> Validator {
    Arc::new(
        |_spec, ctx| match ctx.metadata.get("value").and_then(|v| v.as_f64()) {
            Some(n) if n > 0.0 => Ok(None),
            _ => Err(ValidationError::new("value must be a positive number")),
        },
    )
}

/// A validation config with the `positive` validator registered and the
/// undeclared-spec toggle set as requested.
fn validation_config(reject_undeclared: bool) -> Arc<ValidationConfig> {
    let mut registry = ValidationRegistry::new();
    registry.register(Spec::new("positive"), positive_validator());
    Arc::new(ValidationConfig {
        registry,
        reject_undeclared_specs: reject_undeclared,
    })
}

async fn build_app(validation: Arc<ValidationConfig>) -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
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
        validation,
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

/// CREATE: a registered validator accepts a conforming node and rejects a
/// non-conforming one (upstream `_create_node` → `validate_specs`).
#[tokio::test]
async fn create_runs_registered_validator_reject_and_accept() {
    let (app, _dir) = build_app(validation_config(false)).await;

    // Accept: value > 0.
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "good",
            "structure_family": "container",
            "metadata": {"value": 5},
            "specs": [{"name": "positive"}],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "conforming create: {body}");

    // Reject: value <= 0 → 400 with upstream's detail shape.
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "bad",
            "structure_family": "container",
            "metadata": {"value": -1},
            "specs": [{"name": "positive"}],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "non-conforming create: {body}"
    );
    // tiled-rs wraps every error in `{error: {code, message}}` (its uniform
    // envelope); the message carries upstream's `validate_specs` detail string.
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("failed validation for the positive spec"),
        "message must name the failing spec, got: {message}"
    );
}

/// PATCH: the validator runs on the FINAL merged metadata — reject then accept.
#[tokio::test]
async fn patch_update_runs_registered_validator() {
    let (app, _dir) = build_app(validation_config(false)).await;

    // Seed a conforming node carrying the `positive` spec.
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "n",
            "structure_family": "container",
            "metadata": {"value": 5},
            "specs": [{"name": "positive"}],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // PATCH the metadata to a non-conforming value → rejected.
    let (status, body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/n",
        serde_json::json!({
            "content-type": "application/merge-patch+json",
            "metadata": {"value": -3},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "patch to invalid: {body}");

    // PATCH to a conforming value → accepted.
    let (status, body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/n",
        serde_json::json!({
            "content-type": "application/merge-patch+json",
            "metadata": {"value": 9},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "patch to valid: {body}");
}

/// PUT: the validator runs on the wholesale-replacement metadata — reject then
/// accept.
#[tokio::test]
async fn put_update_runs_registered_validator() {
    let (app, _dir) = build_app(validation_config(false)).await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "m",
            "structure_family": "container",
            "metadata": {"value": 5},
            "specs": [{"name": "positive"}],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // PUT specs+metadata with a non-conforming value → rejected.
    let (status, body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/m",
        serde_json::json!({
            "metadata": {"value": -2},
            "specs": [{"name": "positive"}],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "put to invalid: {body}");

    // PUT with a conforming value → accepted.
    let (status, body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/m",
        serde_json::json!({
            "metadata": {"value": 12},
            "specs": [{"name": "positive"}],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "put to valid: {body}");
}

/// An undeclared spec (no registered validator) passes when
/// `reject_undeclared_specs` is off.
#[tokio::test]
async fn undeclared_spec_passes_when_reject_off() {
    let (app, _dir) = build_app(validation_config(false)).await;

    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "u",
            "structure_family": "container",
            "metadata": {},
            "specs": [{"name": "mystery"}],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "undeclared spec, reject off: {body}"
    );
}

/// An undeclared spec is a 400 when `reject_undeclared_specs` is on, with
/// upstream's `Unrecognized spec: <name>` detail.
#[tokio::test]
async fn undeclared_spec_rejected_when_reject_on() {
    let (app, _dir) = build_app(validation_config(true)).await;

    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "u",
            "structure_family": "container",
            "metadata": {},
            "specs": [{"name": "mystery"}],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "undeclared spec, reject on: {body}"
    );
    assert_eq!(
        body["error"]["message"].as_str().unwrap_or_default(),
        "Unrecognized spec: mystery"
    );
}

/// Finding 4 (w30): a bare-STRING spec on PATCH must reach `validate_specs`
/// exactly like the object form. Before the fix the raw specs value was parsed
/// with an all-or-nothing `serde_json::from_value::<Vec<Spec>>(..)`; a bare
/// string (`"mystery"`) — a valid on-wire spec encoding — failed that parse,
/// collapsed the list to empty, and was persisted WITHOUT rejection even under
/// `reject_undeclared_specs`, while the object form `{"name":"mystery"}` was
/// correctly rejected. The two encodings must now be treated identically.
#[tokio::test]
async fn patch_bare_string_undeclared_spec_rejected_like_object() {
    let (app, _dir) = build_app(validation_config(true)).await;

    // Seed a no-spec node.
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "n",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Bare-string undeclared spec via merge-patch → 400, upstream detail.
    let (bare_status, bare_body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/n",
        serde_json::json!({
            "content-type": "application/merge-patch+json",
            "specs": ["mystery"],
        }),
    )
    .await;
    assert_eq!(
        bare_status,
        StatusCode::BAD_REQUEST,
        "bare-string undeclared spec must be rejected: {bare_body}"
    );
    assert_eq!(
        bare_body["error"]["message"].as_str().unwrap_or_default(),
        "Unrecognized spec: mystery"
    );

    // Object form on the same (the rejected PATCH left it spec-less) node →
    // identical status + message. This is the parity the fix restores.
    let (obj_status, obj_body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/n",
        serde_json::json!({
            "content-type": "application/merge-patch+json",
            "specs": [{"name": "mystery"}],
        }),
    )
    .await;
    assert_eq!(
        obj_status, bare_status,
        "string and object forms must agree on status"
    );
    assert_eq!(
        obj_body["error"]["message"].as_str().unwrap_or_default(),
        bare_body["error"]["message"].as_str().unwrap_or_default(),
        "string and object forms must agree on the rejection message"
    );
}

/// Finding 4 (w30): the same bare-string parity on PUT (wholesale replacement).
#[tokio::test]
async fn put_bare_string_undeclared_spec_rejected_like_object() {
    let (app, _dir) = build_app(validation_config(true)).await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "m",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (bare_status, bare_body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/m",
        serde_json::json!({
            "metadata": {},
            "specs": ["mystery"],
        }),
    )
    .await;
    assert_eq!(
        bare_status,
        StatusCode::BAD_REQUEST,
        "bare-string undeclared spec must be rejected on PUT: {bare_body}"
    );
    assert_eq!(
        bare_body["error"]["message"].as_str().unwrap_or_default(),
        "Unrecognized spec: mystery"
    );

    let (obj_status, obj_body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/m",
        serde_json::json!({
            "metadata": {},
            "specs": [{"name": "mystery"}],
        }),
    )
    .await;
    assert_eq!(
        obj_status, bare_status,
        "string and object forms must agree on status"
    );
    assert_eq!(
        obj_body["error"]["message"].as_str().unwrap_or_default(),
        bare_body["error"]["message"].as_str().unwrap_or_default(),
        "string and object forms must agree on the rejection message"
    );
}

/// Finding 4 (w30): with `reject_undeclared_specs` OFF, a bare-string undeclared
/// spec is still accepted — the fix only routes every spec THROUGH validation;
/// it does not itself reject undeclared specs when the toggle is off.
#[tokio::test]
async fn patch_bare_string_undeclared_spec_accepted_when_reject_off() {
    let (app, _dir) = build_app(validation_config(false)).await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "n",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/n",
        serde_json::json!({
            "content-type": "application/merge-patch+json",
            "specs": ["mystery"],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "bare-string undeclared spec, reject off → accepted: {body}"
    );
}

/// Finding 7 (w30): CREATE rejects duplicate specs with 422, matching upstream
/// `PostMetadataRequest.specs_uniqueness_validator` (schemas.py:471-478). The
/// catalog's `validate_payload` caps the spec COUNT but not uniqueness, so the
/// handler must — and it does so before registry validation, so the duplicate
/// is a 422 even with `reject_undeclared_specs` off (specs "a" are undeclared).
#[tokio::test]
async fn create_rejects_duplicate_specs() {
    let (app, _dir) = build_app(validation_config(false)).await;
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "dup",
            "structure_family": "container",
            "metadata": {},
            "specs": [{"name": "a"}, {"name": "a"}],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "duplicate specs must be 422: {body}"
    );
}

/// Finding 7 boundary: the same spec NAME with DISTINCT versions is not a
/// duplicate — uniqueness is by `(name, version)`, matching upstream `Spec`
/// equality and the `spec_identity` the PATCH/PUT handlers use.
#[tokio::test]
async fn create_allows_same_name_distinct_versions() {
    let (app, _dir) = build_app(validation_config(false)).await;
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "ver",
            "structure_family": "container",
            "metadata": {},
            "specs": [{"name": "a", "version": "1"}, {"name": "a", "version": "2"}],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "same name with distinct versions is unique: {body}"
    );
}

/// Finding 9 (w30): a POST under a NON-EXISTENT parent path is a 404, even when
/// the request also carries an undeclared spec that `reject_undeclared_specs`
/// would otherwise 400. Upstream resolves the parent `entry` before
/// `_create_node` runs `validate_specs`, so parent-missing precedes spec
/// validation; tiled-rs now validates specs only after parent resolution.
#[tokio::test]
async fn create_under_missing_parent_is_404_not_spec_400() {
    let (app, _dir) = build_app(validation_config(true)).await;
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/does_not_exist",
        serde_json::json!({
            "key": "child",
            "structure_family": "container",
            "metadata": {},
            "specs": [{"name": "mystery"}],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "missing parent must 404 before undeclared-spec 400: {body}"
    );
}

/// Finding 9 companion: when the parent DOES exist, an undeclared spec still
/// 400s under reject-on — sequencing spec validation after parent resolution
/// does not disable it.
#[tokio::test]
async fn create_under_existing_parent_still_400s_undeclared_spec() {
    let (app, _dir) = build_app(validation_config(true)).await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "parent",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/parent",
        serde_json::json!({
            "key": "child",
            "structure_family": "container",
            "metadata": {},
            "specs": [{"name": "mystery"}],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "undeclared spec under an existing parent → 400: {body}"
    );
    assert_eq!(
        body["error"]["message"].as_str().unwrap_or_default(),
        "Unrecognized spec: mystery"
    );
}

/// A node carrying NO specs is unaffected whether `reject_undeclared_specs` is
/// on or off — nothing to validate, nothing to reject.
#[tokio::test]
async fn no_spec_node_unaffected_either_way() {
    for reject in [false, true] {
        let (app, _dir) = build_app(validation_config(reject)).await;
        let (status, body) = json_request(
            &app,
            Method::POST,
            "/api/v1/metadata/",
            serde_json::json!({
                "key": "plain",
                "structure_family": "container",
                "metadata": {"anything": 1},
                "specs": [],
                "data_sources": [],
            }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "no-spec node must be unaffected (reject_undeclared={reject}): {body}"
        );

        // And a metadata update on a no-spec node is likewise unaffected.
        let (status, body) = json_request(
            &app,
            Method::PATCH,
            "/api/v1/metadata/plain",
            serde_json::json!({
                "content-type": "application/merge-patch+json",
                "metadata": {"anything": 2},
            }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "no-spec update must be unaffected (reject_undeclared={reject}): {body}"
        );
    }
}

/// Task #119 (w31) — READ-BACK normalization, router single-node path
/// (`GET /api/v1/metadata/{path}` → `catalog_metadata_resource`). A persisted
/// bare-string spec element (reachable with `reject_undeclared_specs` off) must
/// come back normalized to `{name, version: null}`, NOT collapse the whole
/// `specs` list to `None`. Before the fix the read decoded with an
/// all-or-nothing `serde_json::from_value::<Option<Vec<Spec>>>(..)`, so one
/// bare-string element made the ENTIRE list deserialize fail and vanish.
#[tokio::test]
async fn get_metadata_normalizes_persisted_bare_string_spec() {
    let (app, _dir) = build_app(validation_config(false)).await;

    // Seed a no-spec node, then PATCH a BARE-STRING spec. With reject off the
    // undeclared spec is accepted; wave-34 (F2) NORMALIZES it to `{name}` at
    // write, and the read path (`Spec::parse_stored_list`) likewise normalizes a
    // genuine out-of-band bare-string row — either way a bare string reads back
    // as an object and never collapses the list (the read-side guard for raw
    // bare-string rows is pinned by the `parse_stored_list` unit tests).
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "rb",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/rb",
        serde_json::json!({
            "content-type": "application/merge-patch+json",
            "specs": ["mystery"],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "persist bare-string spec: {body}");

    // Read it back: the bare string is normalized to an object, the list is
    // present (not None), and `version` is omitted (Spec serializes `None`
    // versions away — the same shape an object-form spec with no version gets).
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/rb",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "read back: {body}");
    let specs = body["data"]["attributes"]["specs"]
        .as_array()
        .unwrap_or_else(|| {
            panic!("specs must be a present array, not None: {body}");
        });
    assert_eq!(
        specs,
        &vec![serde_json::json!({"name": "mystery"})],
        "bare-string spec must read back normalized as an object: {body}"
    );
}

/// Task #119 (w31) — a MIXED list of bare-string and object specs must read
/// back FULLY and IN ORDER through the router path: no element is dropped, the
/// object element keeps its `version`, and the ordering is preserved. This is
/// the boundary the old all-or-nothing decode could not represent — one bad
/// element took every sibling with it.
#[tokio::test]
async fn get_metadata_preserves_mixed_string_and_object_specs_in_order() {
    let (app, _dir) = build_app(validation_config(false)).await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "mix",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Persist string, versioned-object, string — interleaved, so a stable
    // element-wise parse is the only way the order and the middle version
    // survive.
    let (status, body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/mix",
        serde_json::json!({
            "content-type": "application/merge-patch+json",
            "specs": ["alpha", {"name": "beta", "version": "2"}, "gamma"],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "persist mixed specs: {body}");

    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/mix",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "read back mixed: {body}");
    let specs = &body["data"]["attributes"]["specs"];
    assert_eq!(
        specs,
        &serde_json::json!([
            {"name": "alpha"},
            {"name": "beta", "version": "2"},
            {"name": "gamma"},
        ]),
        "mixed specs must read back fully and in order: {body}"
    );
}

/// Task #119 (w31) — READ-BACK normalization, catalog-adapter SEARCH path
/// (`GET /api/v1/search/{path}` → `CatalogAdapter::search_page` → `SearchEntry`).
/// This is the second read site that decoded stored specs; before the fix it
/// deliberately mirrored the metadata endpoint's all-or-nothing decode, so it
/// dropped the whole list on a bare-string element too. A search row must now
/// carry the same normalized specs its metadata row does.
#[tokio::test]
async fn search_normalizes_persisted_bare_string_specs() {
    let (app, _dir) = build_app(validation_config(false)).await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "sc",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/sc",
        serde_json::json!({
            "content-type": "application/merge-patch+json",
            "specs": ["alpha", {"name": "beta", "version": "2"}],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "persist search specs: {body}");

    // Search root's children and locate the seeded node's entry. No `fields`
    // query means the full entry (specs included).
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/search/",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search: {body}");
    let entry = body["data"]
        .as_array()
        .and_then(|rows| rows.iter().find(|r| r["id"] == "sc"))
        .unwrap_or_else(|| panic!("search must return the seeded node: {body}"));
    assert_eq!(
        entry["attributes"]["specs"],
        serde_json::json!([
            {"name": "alpha"},
            {"name": "beta", "version": "2"},
        ]),
        "search row must carry the same normalized specs as its metadata row: {body}"
    );
}

/// Wave-33 (A1) — WRITE-SIDE TIGHTENING, merge-patch flavor. A spec whose
/// `version` is present but NOT a string cannot round-trip losslessly through
/// `Spec::parse_stored_list` (the version is dropped on read-back), so the write
/// is now rejected with 422 at the request boundary — mirroring upstream's typed
/// `PatchMetadataRequest.specs: Specs` (`List[Spec]`, schemas.py:575) and our own
/// POST path (`PostMetadataRequest.specs: Vec<Spec>`). Before the fix this PATCH
/// returned 200 and the node silently stored `version: 5`, which then vanished on
/// every parsed read. The read-back-name-only behavior for a genuinely
/// out-of-band row stays pinned at the `parse_stored_list` unit level
/// (`src/core/structures.rs` tests); it is no longer reachable through the API.
#[tokio::test]
async fn patch_merge_rejects_non_string_version() {
    let (app, _dir) = build_app(validation_config(false)).await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "nv",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/nv",
        serde_json::json!({
            "content-type": "application/merge-patch+json",
            "specs": [{"name": "x", "version": 5}],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "merge-patch with a non-string version must be 422: {body}"
    );

    // The rejected write must not have landed: specs stay empty.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/nv",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "read back: {body}");
    assert_eq!(
        body["data"]["attributes"]["specs"],
        serde_json::json!([]),
        "a rejected non-string-version PATCH must not mutate specs: {body}"
    );
}

/// Wave-33 (A1) — WRITE-SIDE TIGHTENING, json-patch flavor. The same invariant
/// on the RFC 6902 path: an `add`/`replace` op whose value carries a non-string
/// `version` is rejected with 422, mirroring upstream `List[JSONPatchSpec]` whose
/// op `value` is typed as `Spec` (schemas.py:557/575). A name-bearing sibling in
/// the same op batch does not rescue it — the whole write is rejected and the
/// node's specs stay untouched.
#[tokio::test]
async fn patch_json_patch_rejects_non_string_version() {
    let (app, _dir) = build_app(validation_config(false)).await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "gv",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/gv",
        serde_json::json!({
            "content-type": "application/json-patch+json",
            "specs": [
                {"op": "add", "path": "/-", "value": "good"},
                {"op": "add", "path": "/-", "value": {"name": "y", "version": 5}},
            ],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "json-patch adding a non-string version must be 422: {body}"
    );

    // The rejected write must not have landed: specs stay empty.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/gv",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "read back: {body}");
    assert_eq!(
        body["data"]["attributes"]["specs"],
        serde_json::json!([]),
        "a rejected non-string-version json-patch must not mutate specs: {body}"
    );
}

/// Task #119 follow-up (w31) — READ-BACK normalization, REVISIONS path
/// (`GET /api/v1/revisions/{path}`). A revision whose stored specs snapshot
/// holds a bare-string element must read back normalized to
/// `{name, version: null}`, in order alongside object-form siblings — matching
/// upstream `Revision.specs: Specs` (a typed `List[Spec]`, schemas.py). The
/// endpoint used to emit the raw stored JSON, so a bare string leaked through
/// as `"mystery"` instead of the object form.
#[tokio::test]
async fn revisions_normalize_persisted_bare_string_specs() {
    let (app, _dir) = build_app(validation_config(false)).await;

    // Seed a no-spec node.
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "rev",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // PATCH #1 sets a MIXED bare-string + object specs list. This pushes the
    // pre-update state (empty specs) as revision 1 and makes the mixed list the
    // node's current specs.
    let (status, body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/rev",
        serde_json::json!({
            "content-type": "application/merge-patch+json",
            "specs": ["mystery", {"name": "beta", "version": "2"}],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "persist mixed specs: {body}");

    // PATCH #2 is a metadata-only change. It pushes the PREVIOUS state — the
    // mixed specs snapshot — as revision 2, which is the row under test.
    let (status, body) = json_request(
        &app,
        Method::PATCH,
        "/api/v1/metadata/rev",
        serde_json::json!({
            "content-type": "application/merge-patch+json",
            "metadata": {"x": 1},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "metadata-only patch: {body}");

    // Read the revision history: revision 2's specs snapshot must be normalized.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/revisions/rev",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "list revisions: {body}");
    let rev2 = body["data"]
        .as_array()
        .and_then(|rows| rows.iter().find(|r| r["revision_number"] == 2))
        .unwrap_or_else(|| panic!("revision 2 must be present: {body}"));
    assert_eq!(
        rev2["attributes"]["specs"],
        serde_json::json!([
            {"name": "mystery"},
            {"name": "beta", "version": "2"},
        ]),
        "revision specs must read back normalized and in order: {body}"
    );
}

/// Wave-33 (A1) — WRITE-SIDE TIGHTENING, PUT (wholesale replace). A non-string
/// version in the replacement specs is rejected with 422, mirroring upstream
/// `PutMetadataRequest.specs: Optional[Specs]` (schemas.py:528). The prior specs
/// must be preserved.
#[tokio::test]
async fn put_rejects_non_string_version() {
    let (app, _dir) = build_app(validation_config(false)).await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "pv",
            "structure_family": "container",
            "metadata": {},
            "specs": [{"name": "keep"}],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/pv",
        serde_json::json!({
            "metadata": {},
            "specs": [{"name": "x", "version": 5}],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "PUT with a non-string version must be 422: {body}"
    );

    // The rejected replacement must not have landed: the original spec stays.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/pv",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "read back: {body}");
    assert_eq!(
        body["data"]["attributes"]["specs"],
        serde_json::json!([{"name": "keep"}]),
        "a rejected PUT must not mutate specs: {body}"
    );
}

/// Wave-33 (A1) — a CONFORMANT bare-string spec on PUT/PATCH is STILL accepted:
/// it round-trips losslessly through `parse_stored_list` (arm 1), so the write
/// tightening must not reject it (only non-string name/version is rejected). This
/// guards the tiled-rs bare-string back-compat the w30 tests established.
#[tokio::test]
async fn put_bare_string_spec_still_accepted_after_tightening() {
    let (app, _dir) = build_app(validation_config(false)).await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "bs",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/bs",
        serde_json::json!({
            "metadata": {},
            "specs": ["legacy"],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a lossless bare-string spec must remain writable: {body}"
    );
}

/// Wave-33 (A1) — regression guard + read-path agreement. A CONFORMANT
/// string-version spec is stored and read back byte-identically, and the parsed
/// metadata read agrees with the raw `distinct` facet for API-written data (the
/// cross-endpoint inconsistency the version-drop bug caused can no longer arise,
/// because a non-string version is now unwritable).
#[tokio::test]
async fn conformant_version_round_trips_and_distinct_agrees() {
    let (app, _dir) = build_app(validation_config(false)).await;

    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "ok",
            "structure_family": "container",
            "metadata": {},
            "specs": [{"name": "x", "version": "1"}],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create: {body}");

    // Parsed metadata read: byte-identical to what was written.
    let (status, meta) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/ok",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "read back: {meta}");
    let metadata_specs = meta["data"]["attributes"]["specs"].clone();
    assert_eq!(
        metadata_specs,
        serde_json::json!([{"name": "x", "version": "1"}]),
        "conformant version must round-trip identically: {meta}"
    );

    // Raw `distinct` facet must carry the SAME specs value.
    let (status, dist) = json_request(
        &app,
        Method::GET,
        "/api/v1/distinct/?specs=true",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "distinct: {dist}");
    let distinct_value = dist["specs"][0]["value"].clone();
    assert_eq!(
        distinct_value, metadata_specs,
        "distinct facet and parsed metadata read must agree for API-written data: {dist}"
    );
}

/// Wave-34 (F2) — a PUT spec object carrying EXTRA keys beyond `{name, version}`
/// must be NORMALIZED at write, not stored verbatim. Before the fix
/// `validate_writable_specs` only type-checked `name`/`version` and the raw JSON
/// (including `foo`) was persisted, so `GET /metadata` (parsed through
/// `Spec::parse_stored_list`, which drops `foo`) and `GET /distinct?specs=true`
/// (raw column) disagreed — the exact cross-endpoint inconsistency #131 set out
/// to close, still reachable via extra keys. Upstream types the body as
/// `List[Spec]` (name/version only), so extra keys are never persisted. The
/// write still 200s; the stored value round-trips losslessly through
/// `parse_stored_list` by construction.
#[tokio::test]
async fn put_extra_key_spec_is_normalized_and_distinct_agrees() {
    let (app, _dir) = build_app(validation_config(false)).await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "xk",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // PUT a spec object with an extra `foo` key alongside a conformant
    // name/version. The write is accepted (200) — extra keys are dropped, not
    // rejected — matching upstream's `List[Spec]` parse.
    let (status, body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/xk",
        serde_json::json!({
            "metadata": {},
            "specs": [{"name": "x", "version": "1", "foo": "bar"}],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "extra-key spec must still 200: {body}"
    );

    // Parsed metadata read: normalized to {name, version}, no `foo`.
    let (status, meta) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/xk",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "read back: {meta}");
    let metadata_specs = meta["data"]["attributes"]["specs"].clone();
    assert_eq!(
        metadata_specs,
        serde_json::json!([{"name": "x", "version": "1"}]),
        "extra keys must be normalized away on the metadata read: {meta}"
    );

    // Raw `distinct` facet must carry the SAME normalized value (no `foo`).
    let (status, dist) = json_request(
        &app,
        Method::GET,
        "/api/v1/distinct/?specs=true",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "distinct: {dist}");
    let distinct_value = dist["specs"][0]["value"].clone();
    assert_eq!(
        distinct_value, metadata_specs,
        "distinct facet and parsed metadata read must agree — extra keys must not \
         leak into the raw stored specs: {dist}"
    );
}

/// Wave-34 (F3) — upstream bounds a spec `version` to `max_length=255`, exactly
/// as it bounds `name` (`StringConstraints`, `structures/core.py:29-30`). The
/// port bounded only the NAME length (`validate_payload`, `catalog/node.rs:317`)
/// and never the version, so a 256-char version was accepted (a validation
/// strictness divergence). Boundary: 255 chars accepted, 256 rejected with 422.
#[tokio::test]
async fn put_spec_version_length_bounded_at_255() {
    let (app, _dir) = build_app(validation_config(false)).await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "vl",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Boundary: a 255-char version is accepted.
    let v255 = "a".repeat(255);
    let (status, body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/vl",
        serde_json::json!({
            "metadata": {},
            "specs": [{"name": "x", "version": v255}],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "255-char version must be accepted: {body}"
    );

    // A 256-char version exceeds the upstream bound → 422.
    let v256 = "a".repeat(256);
    let (status, body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/vl",
        serde_json::json!({
            "metadata": {},
            "specs": [{"name": "x", "version": v256}],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "256-char version must be rejected 422: {body}"
    );
}

/// Wave-35 (F3) — a spec `name`/`version` length bound must count CHARACTERS
/// (Unicode code points), not BYTES. Upstream `StringConstraints(max_length=255)`
/// counts code points (Python `len(str)`, `structures/core.py:29-30`), so a
/// 200-CHARACTER multibyte string (≤255 chars) is accepted even though its UTF-8
/// encoding is 600 bytes. The port compared `str::len()` (bytes) at
/// `catalog/node.rs:317` (name) and `:327` (version), so a 200-char CJK name or
/// version was wrongly 422'd (RED before this fix). The ASCII 256-char boundary
/// still rejects.
#[tokio::test]
async fn spec_name_and_version_bounded_by_chars_not_bytes() {
    let (app, _dir) = build_app(validation_config(false)).await;

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "mb",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // 200 CJK code points = 200 chars (≤255) but 600 UTF-8 bytes (>255). Upstream
    // accepts on char count; the port must too. RED before: byte length 600 > 255.
    let name_mb = "가".repeat(200);
    let ver_mb = "나".repeat(200);
    assert_eq!(name_mb.chars().count(), 200);
    assert!(
        name_mb.len() > 255,
        "precondition: 200 CJK chars must exceed 255 UTF-8 bytes"
    );

    // Multibyte NAME (≤255 chars) must be accepted.
    let (status, body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/mb",
        serde_json::json!({
            "metadata": {},
            "specs": [{"name": name_mb}],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "200-char multibyte name (≤255 chars, >255 bytes) must be accepted: {body}"
    );

    // Multibyte VERSION (≤255 chars) must be accepted.
    let (status, body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/mb",
        serde_json::json!({
            "metadata": {},
            "specs": [{"name": "x", "version": ver_mb}],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "200-char multibyte version (≤255 chars, >255 bytes) must be accepted: {body}"
    );

    // Boundary guard: a 256-CHAR ASCII name (256 chars) still 422s.
    let ascii256 = "a".repeat(256);
    let (status, body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/mb",
        serde_json::json!({
            "metadata": {},
            "specs": [{"name": ascii256}],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "256-char name must still be rejected 422: {body}"
    );

    // Boundary guard: a 256-CHAR ASCII version still 422s.
    let (status, body) = json_request(
        &app,
        Method::PUT,
        "/api/v1/metadata/mb",
        serde_json::json!({
            "metadata": {},
            "specs": [{"name": "x", "version": "a".repeat(256)}],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "256-char version must still be rejected 422: {body}"
    );
}
