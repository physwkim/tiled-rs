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
