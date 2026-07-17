//! Single-user API-key mode: scope grant + multi-user mode-exclusivity.
//!
//! Two invariants, both mirroring upstream `get_scopes_from_api_key`
//! (`tiled/server/authentication.py:347-381`):
//!
//! 1. The single-user key grants exactly `SINGLE_USER_SCOPES`
//!    (`tiled/access_control/scopes.py:32-46`) — node I/O + metrics + webhook
//!    scopes, but NOT the credential/principal-management scopes or the
//!    `admin` superscope.
//! 2. In multi-user mode (an auth DB is configured) the single-user key is
//!    inert — upstream consults it only inside `if not authenticated:`
//!    (`authentication.py:350`). tiled-rs enforces this by construction in
//!    `build_app`, so a mixed-mode misconfiguration cannot become an
//!    `admin` backdoor.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use tiled_rs::auth::{AuthDb, Issuer, Scope};
use tiled_rs::catalog::Catalog;
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;
use tiled_rs::server::AppState;

const SECRET: &str = "single-user-secret-key-0123456789";

/// Build an `AppState`. `with_auth_db = true` puts the server in multi-user
/// mode (an auth DB is configured); `api_key` sets the single-user key. The
/// interesting case for mode-exclusivity is *both* set.
async fn make_state(api_key: Option<String>, with_auth_db: bool) -> (AppState, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&cat_uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let (auth_db, issuer) = if with_auth_db {
        let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());
        let auth_db = AuthDb::connect(&auth_uri).await.unwrap();
        auth_db.migrate().await.unwrap();
        let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
        (Some(auth_db), Some(issuer))
    } else {
        (None, None)
    };

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> =
        Arc::new(tiled_rs::catalog::adapter::UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_rs::serialization::default_registry());

    let state = AppState {
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
        api_key,
        catalog: Some(catalog.clone()),
        auth_db,
        issuer,
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
    };
    (state, dir)
}

/// Invariant 1: a genuine single-user key (no auth DB) authenticates and is
/// granted exactly `SINGLE_USER_SCOPES` — never the `admin` superscope or the
/// credential/principal-management scopes.
#[tokio::test]
async fn single_user_key_grants_single_user_scopes_not_full() {
    let (state, _dir) = make_state(Some(SECRET.into()), false).await;

    let ctx = tiled_rs::server::app::validate_apikey(&state, SECRET)
        .await
        .expect("single-user key should authenticate");
    assert_eq!(ctx.kind, tiled_rs::server::AuthKind::SingleUserKey);

    // The 11 upstream SINGLE_USER_SCOPES are granted.
    for sc in [
        Scope::ReadMetadata,
        Scope::ReadData,
        Scope::WriteMetadata,
        Scope::WriteData,
        Scope::DeleteRevision,
        Scope::DeleteNode,
        Scope::CreateNode,
        Scope::Register,
        Scope::Metrics,
        Scope::ReadWebhooks,
        Scope::WriteWebhooks,
    ] {
        assert!(
            ctx.scopes.contains(sc),
            "single-user key must grant {}",
            sc.as_str()
        );
    }

    // ...but NOT the credential/principal-management scopes or `admin`
    // (upstream `SINGLE_USER_SCOPES` excludes them). Granting `full()` here is
    // the pre-fix defect this pins.
    for sc in [
        Scope::CreateApiKeys,
        Scope::RevokeApiKeys,
        Scope::AdminApiKeys,
        Scope::ReadPrincipals,
        Scope::WritePrincipals,
        Scope::Admin,
    ] {
        assert!(
            !ctx.scopes.contains(sc),
            "single-user key must NOT grant {}",
            sc.as_str()
        );
    }
}

/// Invariant 2: in multi-user mode (auth DB configured) the single-user key is
/// ignored, even when both are set — the mixed-mode misconfiguration must not
/// become a full-`admin` backdoor. `build_app` drops the key by construction.
#[tokio::test]
async fn multi_user_mode_ignores_single_user_key() {
    // Mixed-mode: an auth DB is configured AND a single-user key is set.
    let (state, _dir) = make_state(Some(SECRET.into()), true).await;
    let app = tiled_rs::server::build_app(state);

    // Presenting the single-user secret must NOT authenticate against a
    // multi-user server (pre-fix: the fall-through granted full() → 200).
    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/metadata/")
        .header("authorization", format!("Apikey {SECRET}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "single-user key must be ignored when an auth DB is configured"
    );
}
