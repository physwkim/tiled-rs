//! Route handlers for the Tiled API.

use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::Arc;

use axum::Json;
use axum::extract::{OriginalUri, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use serde::Deserialize;

use crate::server::extractors::PathSegments;

use crate::core::adapters::{AnyAdapter, ContainerAdapter};
use crate::core::links;
use crate::core::schemas::{About, AboutAuthentication, Response, SortDirection};

use crate::server::core;
use crate::server::error::ServerError;
use crate::server::extractors::BaseUrl;
use crate::server::state::AppState;

/// Helper that turns axum's [`OriginalUri`] into a list of percent-decoded
/// path segments after stripping the API prefix.
fn segments_from_uri(uri: &axum::http::Uri, prefix: &str) -> Vec<String> {
    PathSegments::from_raw_path(uri.path(), prefix).0
}

/// Strip whichever creation-route prefix the request used. `POST /register/{path}`
/// and its asset-free alias `POST /metadata/{path}` (Python parity: router.py:1769)
/// share one create core; the child path is the suffix after the matched prefix.
fn create_segments_from_uri(uri: &axum::http::Uri) -> Vec<String> {
    if uri.path().contains("/api/v1/metadata/") {
        segments_from_uri(uri, "/api/v1/metadata/")
    } else {
        segments_from_uri(uri, "/api/v1/register/")
    }
}

/// Walk `segments` through the catalog (when present) or the in-memory
/// tree, apply the per-node access policy at each level, and verify the
/// caller's narrowed scopes include `required_scope`.
///
/// 404 when the path is missing OR when the policy narrows the caller to
/// no `ReadMetadata` at any level (Python SecureEntry: "if you can't read
/// metadata the node doesn't exist for you").
/// 403 when the final narrowed context lacks `required_scope`.
/// Returns the final narrowed [`AuthContext`].
pub(crate) async fn resolve_entry(
    state: &AppState,
    auth: crate::server::AuthContext,
    segments: &[String],
    required_scope: crate::auth::Scope,
) -> Result<crate::server::AuthContext, ServerError> {
    if segments.is_empty() {
        auth.require(required_scope)?;
        return Ok(auth);
    }
    let narrowed = if state.catalog.is_some() {
        resolve_entry_catalog(state, auth, segments).await?
    } else {
        resolve_entry_tree(state, auth, segments).await?
    };
    narrowed.require(required_scope)?;
    Ok(narrowed)
}

/// Catalog-backed path: call `catalog.lookup` for each prefix of `segments`,
/// narrow auth at each level (404 when the narrowed scopes lose ReadMetadata).
pub(crate) async fn resolve_entry_catalog(
    state: &AppState,
    mut auth: crate::server::AuthContext,
    segments: &[String],
) -> Result<crate::server::AuthContext, ServerError> {
    let catalog = state
        .catalog
        .as_ref()
        .expect("resolve_entry_catalog requires catalog");
    let mut prev_was_table = false;
    for i in 0..segments.len() {
        let prefix = &segments[..=i];
        let node = match catalog.lookup(prefix).await.map_err(map_catalog_err)? {
            Some(node) => node,
            None => {
                // A `[table, column]` path has no DB node for the column: the
                // column is a synthesized array child of the table leaf.
                // Upstream `lookup_adapter` falls back to `adapter.get(segment)`
                // on the deepest data-source-backed node when the DB lookup
                // misses (catalog/adapter.py:557-566). Mirror that just for the
                // final segment of a table: the column inherits the table's
                // already-narrowed access, so the current context stands. The
                // route's `walk_tree` (→ `core::table_column_as_array`) is the
                // single point that 404s a column absent from the schema, so
                // the auth gate must not 404 a valid column here.
                if i + 1 == segments.len() && prev_was_table {
                    return Ok(auth);
                }
                return Err(ServerError::NotFound(format!(
                    "'{}' not found",
                    segments.join("/")
                )));
            }
        };
        auth = auth
            .narrow_for_node(
                state.access_policy.as_deref(),
                crate::access::NodeContext {
                    path: prefix,
                    structure_family: &node.structure_family,
                    metadata: &node.metadata,
                    access_blob: &node.access_blob,
                },
            )
            .await;
        if !auth.scopes.contains(crate::auth::Scope::ReadMetadata) {
            return Err(ServerError::NotFound(format!(
                "'{}' not found",
                segments.join("/")
            )));
        }
        prev_was_table = node.structure_family == "table";
    }
    Ok(auth)
}

/// In-memory tree path: walk the tree to verify existence (the async
/// `ContainerAdapter` resolves each hop on the executor), then narrow at the
/// terminal node with empty access_blob (in-memory adapters carry no per-node
/// blob).
async fn resolve_entry_tree(
    state: &AppState,
    auth: crate::server::AuthContext,
    segments: &[String],
) -> Result<crate::server::AuthContext, ServerError> {
    let adapter = core::walk_tree(state.root_tree.as_ref(), segments).await?;
    let sf = adapter.structure_family().to_string();
    let metadata = adapter.metadata().clone();

    let access_blob = serde_json::Value::Object(Default::default());
    let narrowed = auth
        .narrow_for_node(
            state.access_policy.as_deref(),
            crate::access::NodeContext {
                path: segments,
                structure_family: &sf,
                metadata: &metadata,
                access_blob: &access_blob,
            },
        )
        .await;
    if !narrowed.scopes.contains(crate::auth::Scope::ReadMetadata) {
        return Err(ServerError::NotFound(format!(
            "'{}' not found",
            segments.join("/")
        )));
    }
    Ok(narrowed)
}

// ---------------------------------------------------------------------------
// Operational endpoints
// ---------------------------------------------------------------------------

pub async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

/// `GET /healthz` — the well-known Kubernetes/container liveness path.
/// Upstream `tiled` serves this exact path + body (`app.py:262-264`,
/// "Standardized for Kubernetes, but also used by other systems.");
/// operators porting a deployment from Python tiled expect it to keep
/// working. Kept distinct from `health`/`ready` above (Rust-native paths
/// with their own response shapes) rather than aliased, so neither side's
/// body drifts if one changes independently.
pub async fn healthz() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ready"}))
}

pub async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    // `.len()` is async — for adapters like tiled_mongo's MongoCatalog the
    // first call triggers a load (offloaded to `spawn_blocking` internally).
    match state.root_tree.len().await {
        Ok(count) => (
            StatusCode::OK,
            Json(serde_json::json!({"status": "ok", "nodes": count})),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(target: "tiled.server", "ready check failed: {e}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"status": "error", "message": "service unavailable"})),
            )
                .into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// GET /api/v1/ — About
// ---------------------------------------------------------------------------

pub async fn about(State(state): State<AppState>, BaseUrl(base_url): BaseUrl) -> impl IntoResponse {
    let formats = state.serialization_registry.all_formats();
    let aliases = state.serialization_registry.all_aliases();

    // Surface configured authenticators so the SPA / CLI can render the right
    // login form. Internal (username/password) authenticators are advertised as
    // `mode=internal` with a `/token`-style auth_endpoint.
    let mut providers: Vec<serde_json::Value> = state
        .authenticators
        .iter()
        .map(|a| {
            serde_json::json!({
                "provider": a.name(),
                "mode": "internal",
                "links": {
                    "auth_endpoint": format!("{base_url}/api/v1/auth/{}/login", a.name()),
                },
            })
        })
        .collect();

    // External OIDC providers wired for an interactive login — i.e. that have
    // BOTH `client_id` and `authorization_endpoint` (the same gate
    // `build_authorize_url` / `build_device_authorize_url` enforce). A
    // bearer-only validator (it only accepts tokens minted elsewhere, with no
    // authorize endpoint) cannot drive a login, so it is intentionally NOT
    // advertised: emitting it as a login provider would be a lie and the
    // client's device grant would POST to an endpoint that 422s.
    //
    // Shape mirrors Python's `OIDCAuthenticator` / `ExternalAuthenticator` spec
    // (router.py:245-253): `mode=external` with `links.auth_endpoint` = tiled's
    // own `/authorize` route. tiled-rs brokers the device flow through that
    // route plus its own `/token`, so — unlike Python's IdP-direct
    // `ProxiedOIDCAuthenticator` — neither `client_id` nor `token_endpoint` is
    // surfaced: advertising `client_id` would switch the tiled-client refresh
    // into the form-encoded OAuth mode this server's JSON-only `/auth/refresh`
    // does not accept, and `token_endpoint` would flip the client's device
    // grant into the IdP-direct OAuth2 variant this server does not serve. The
    // RP-Initiated Logout link (G5) still works without `client_id` via
    // `id_token_hint`.
    if let Some(validator) = state.external_oidc.as_ref() {
        for p in validator.providers() {
            if p.client_id.is_some() && p.authorization_endpoint.is_some() {
                providers.push(serde_json::json!({
                    "provider": p.name.clone(),
                    "mode": "external",
                    "links": {
                        "auth_endpoint":
                            format!("{base_url}/api/v1/auth/provider/{}/authorize", p.name),
                    },
                }));
            }
        }
    }
    // Python: `authentication.required = not settings.allow_anonymous_access`
    // (router.py:205) — purely the anonymous-access policy, NOT whether login
    // providers exist. `AppState::anonymous_scopes()` is the single source of
    // truth for "is an unauthenticated request admitted": it is `Some` when the
    // dev/demo escape hatch (`no_auth_configured()`) OR the operator opt-in
    // (`allow_anonymous_access`) admits anonymous callers, and `None` otherwise.
    // Auth is therefore required iff anonymous admission is `None`. This both
    // fixes the previous `!providers.is_empty()` mistake (which misreported a
    // single-user `api_key`-only server as not requiring auth) and honors
    // `allow_anonymous_access` in multi-user mode, where the old
    // `!no_auth_configured()` reported `required = true` even with the flag set.
    let auth_required = state.anonymous_scopes().is_none();

    // authentication.links — Python router.py:262-278 builds this dict whenever
    // any login path exists (`if provider_specs:`). It is the client's contract:
    // tiled-client `context.rs` reads `whoami`, `refresh_session`,
    // `revoke_session`, and `logout` from it. The only OIDC-specific entry is
    // `logout`: when an external OIDC provider advertises an
    // `end_session_endpoint` (OIDC RP-Initiated Logout 1.0), it is surfaced so
    // the client can end the upstream IdP session (id_token_hint); otherwise
    // `logout` points at tiled's own session-revoking route.
    //
    // Built when an internal authenticator OR an external OIDC provider is
    // configured, mirroring Python's `if provider_specs:` (provider_specs
    // includes OIDC providers there).
    //
    // Deliberate divergence from Python: `refresh_session` always points at
    // tiled's own `/auth/refresh`, never the IdP token endpoint (Python uses the
    // IdP token endpoint for `ProxiedOIDCAuthenticator`). tiled-rs mints its own
    // session tokens for OIDC flows too and the client refreshes them
    // server-side, so the IdP token endpoint would be the wrong target here.
    let oidc_logout = state.external_oidc.as_ref().and_then(|v| {
        v.providers()
            .iter()
            .find_map(|p| p.end_session_endpoint.clone())
    });
    let has_login = !providers.is_empty() || state.external_oidc.is_some();
    let auth_links = has_login.then(|| {
        let logout = oidc_logout.unwrap_or_else(|| format!("{base_url}/api/v1/auth/logout"));
        serde_json::json!({
            "whoami": format!("{base_url}/api/v1/auth/whoami"),
            "apikey": format!("{base_url}/api/v1/auth/apikey"),
            "refresh_session": format!("{base_url}/api/v1/auth/refresh"),
            "revoke_session": format!("{base_url}/api/v1/auth/session/revoke/{{session_id}}"),
            "logout": logout,
        })
    });

    let about = About {
        api_version: 0,
        library_version: env!("CARGO_PKG_VERSION").to_string(),
        formats,
        aliases,
        queries: state.query_names.clone(),
        authentication: AboutAuthentication {
            required: auth_required,
            providers,
            links: auth_links,
        },
        links: HashMap::from([
            ("self".into(), format!("{base_url}/api/v1/")),
            (
                "documentation".into(),
                "https://blueskyproject.io/tiled".into(),
            ),
        ]),
        // Python: `request.scope.get("root_path") or "" + "/api"`, and `+`
        // binds tighter than `or`, so this is `root_path or "/api"` — the
        // no-proxy default is "/api", not "". Rust has no ASGI scope / proxy
        // root_path source plumbed, so we emit the no-proxy default. (A proxy
        // mount prefix override is not yet implemented; see router.py:301.)
        meta: HashMap::from([("root_path".into(), serde_json::Value::String("/api".into()))]),
    };

    Json(about)
}

// ---------------------------------------------------------------------------
// GET /api/v1/metadata/{*path}
// ---------------------------------------------------------------------------

pub async fn metadata_root(
    state: State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    base_url: BaseUrl,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    metadata(
        state,
        OriginalUri("/api/v1/metadata/".parse().expect("static URI")),
        Query(params),
        base_url,
        auth,
    )
    .await
}

/// Apply a JMESPath expression to node metadata, mirroring Python `core.py:480-485`.
///
/// When `select_metadata` is `Some(expr)`, compiles and evaluates the expression
/// against the metadata JSON and returns `{"selected": <result>}`.
/// On compile or evaluation error → `BadRequest` (HTTP 400), matching Python's
/// `JMESPathError → HTTP_400_BAD_REQUEST` in `router.py:395-399 / 503-507`.
/// When `select_metadata` is `None`, returns `metadata` unchanged.
fn apply_select_metadata(
    select_metadata: Option<&str>,
    metadata: Option<serde_json::Value>,
) -> Result<Option<serde_json::Value>, ServerError> {
    let expr_str = match select_metadata {
        None => return Ok(metadata),
        Some(e) => e,
    };
    let expr = jmespath::compile(expr_str).map_err(|e| {
        ServerError::BadRequest(format!(
            "Malformed 'select_metadata' parameter raised JMESPathError: {e}"
        ))
    })?;
    let meta = metadata.unwrap_or(serde_json::Value::Null);
    // Round-trip through JSON string: serde_json::Value → &str → jmespath::Variable.
    // serde_json::to_string on a Value never fails; from_json on its output
    // also never fails, so both conversions are infallible here.
    let json_str =
        serde_json::to_string(&meta).expect("serde_json::Value always serializes to JSON");
    let var = jmespath::Variable::from_json(&json_str)
        .expect("JSON produced by serde_json::to_string always parses");
    let result = expr.search(var).map_err(|e| {
        ServerError::BadRequest(format!(
            "Malformed 'select_metadata' parameter raised JMESPathError: {e}"
        ))
    })?;
    // Variable: Display renders JSON; parse back to serde_json::Value.
    let selected: serde_json::Value =
        serde_json::from_str(&result.to_string()).unwrap_or(serde_json::Value::Null);
    Ok(Some(serde_json::json!({"selected": selected})))
}

/// Apply the `?fields=` projection to one entry's attributes, mirroring Python
/// `EntryFields` (`schemas.py`) as consumed by `construct_resource`
/// (core.py:476-559) and the id-only `fields=""` shape (core.py:248).
///
/// `requested` is the set of `fields` query values the client sent. Each named
/// attribute section is retained only when its `EntryFields` value is present;
/// every other section is set to `None` and dropped by `skip_serializing_if`.
/// `ancestors` is always kept (an id-only `fields=""` resource still carries it,
/// core.py:248). Recognized names: `metadata`, `structure_family`, `structure`,
/// `specs`, `sorting`, `access_blob`. `count` and the empty value (`none`)
/// request no attribute section; unknown names are ignored.
///
/// `data_sources` is deliberately NOT pruned here: upstream sets it from the
/// `include_data_sources` flag alone (core.py:483), independent of `fields`, so
/// a `fields=metadata&include_data_sources=true` request keeps its data sources.
/// It is `None` unless that flag was set, so leaving it untouched is a no-op for
/// every request that did not ask for it.
///
/// The caller MUST invoke this only when the client actually sent `fields` —
/// an absent `fields` means "full entry" (the FastAPI default selects every
/// `EntryFields`), which is the unpruned state and must not be pruned to nothing.
fn prune_entry_fields(attrs: &mut crate::core::schemas::NodeAttributes, requested: &[String]) {
    let want = |f: &str| requested.iter().any(|r| r == f);
    if !want("metadata") {
        attrs.metadata = None;
    }
    if !want("structure_family") {
        attrs.structure_family = None;
    }
    if !want("structure") {
        attrs.structure = None;
    }
    if !want("specs") {
        attrs.specs = None;
    }
    if !want("sorting") {
        attrs.sorting = None;
    }
    if !want("access_blob") {
        attrs.access_blob = None;
    }
}

pub async fn metadata(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    BaseUrl(base_url): BaseUrl,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadMetadata)?;
    // Use the raw URI path so a key containing `%2F` survives as one
    // segment rather than being split apart by axum's `Path<String>` (which
    // percent-decodes before splitting).
    let segments = segments_from_uri(&uri, "/api/v1/metadata/");
    // H2: per-node access policy check at each path segment.
    if !segments.is_empty() {
        let _ = resolve_entry(
            &state,
            auth.clone(),
            &segments,
            crate::auth::Scope::ReadMetadata,
        )
        .await?;
    }
    let include_data_sources = params
        .get("include_data_sources")
        .map(|v| matches!(v.as_str(), "true" | "True" | "1"))
        .unwrap_or(false);
    // ?omit_links=true drops the per-node `links` from the response (Python
    // router.py:461 / core.py:616) — a size optimization, no data change.
    let omit_links = params
        .get("omit_links")
        .map(|v| matches!(v.as_str(), "true" | "True" | "1"))
        .unwrap_or(false);
    let select_metadata = params.get("select_metadata").map(String::as_str);
    // When a SQL catalog is wired, read directly through it: the
    // CatalogAdapter caches children eagerly to satisfy the sync trait,
    // and PATCH/DELETE write past that cache, so a same-request read after
    // a write would otherwise see stale data. Direct DB lookup keeps
    // metadata responses consistent with the latest committed write.
    let mut resource = if let Some(ref catalog) = state.catalog {
        catalog_metadata_resource(
            catalog,
            state.root_tree.as_ref(),
            &segments,
            &base_url,
            include_data_sources,
            i64::try_from(state.exact_count_limit).unwrap_or(i64::MAX),
        )
        .await?
    } else if segments.is_empty() {
        core::construct_root_resource(state.root_tree.as_ref(), &base_url).await?
    } else {
        // The async tree walk resolves each hop on the executor; a blocking
        // adapter (e.g. `MongoCatalog`) offloads its own sync driver to
        // `spawn_blocking` internally, so async workers stay responsive.
        let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
        let id = segments.last().cloned().unwrap_or_default();
        let path = segments.join("/");
        core::construct_resource(&adapter, &id, &path, &base_url).await?
    };

    resource.attributes.metadata =
        apply_select_metadata(select_metadata, resource.attributes.metadata)?;

    if omit_links {
        resource.links = crate::core::schemas::NodeLinks::default();
    }

    Ok(Json(Response {
        data: Some(resource),
        error: None,
        links: None,
        meta: None,
    }))
}

// ---------------------------------------------------------------------------
// GET /api/v1/search/{*path}
// ---------------------------------------------------------------------------

/// Parse the `sort` query parameter(s) into ordered `(key, direction)` pairs,
/// mirroring Python `sorting_param` (dependencies.py:218-255): values are
/// comma-separated; a leading `-` means descending, a leading `+` (or no
/// prefix) ascending. Truly empty items (old clients send a bare `sort=`) are
/// dropped; a bare `-`/`+` yields an empty key — the default-direction
/// sentinel honored by the `id` tiebreaker in `construct_order_by_clauses`.
fn parse_sort(params: &[(String, String)]) -> Vec<(String, SortDirection)> {
    let mut sorting = Vec::new();
    for (_, raw) in params.iter().filter(|(k, _)| k == "sort") {
        for item in raw.split(',') {
            if item.is_empty() {
                continue;
            }
            let (key, dir) = if let Some(rest) = item.strip_prefix('-') {
                (rest, SortDirection::Descending)
            } else if let Some(rest) = item.strip_prefix('+') {
                (rest, SortDirection::Ascending)
            } else {
                (item, SortDirection::Ascending)
            };
            sorting.push((key.to_string(), dir));
        }
    }
    sorting
}

pub async fn search_root(
    state: State<AppState>,
    // Vec<(K,V)> preserves repeated keys so multiple same-type filters all survive.
    Query(params): Query<Vec<(String, String)>>,
    base_url: BaseUrl,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    search(
        state,
        OriginalUri("/api/v1/search/".parse().expect("static URI")),
        Query(params),
        base_url,
        auth,
    )
    .await
}

pub async fn search(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    // Vec<(K,V)> preserves repeated keys so multiple same-type filters all survive.
    Query(params): Query<Vec<(String, String)>>,
    BaseUrl(base_url): BaseUrl,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadMetadata)?;
    let segments = segments_from_uri(&uri, "/api/v1/search/");

    let offset: usize = params
        .iter()
        .find(|(k, _)| k == "page[offset]")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let limit: usize = params
        .iter()
        .find(|(k, _)| k == "page[limit]")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(links::DEFAULT_PAGE_SIZE)
        .min(links::MAX_PAGE_SIZE);
    // Keyset cursor (a node id) the client got from a previous `next` link.
    // Present ⇒ serve the page after it instead of the offset window.
    let cursor: Option<i64> = params
        .iter()
        .find(|(k, _)| k == "page[cursor]")
        .and_then(|(_, v)| v.parse().ok());

    // Parse `sort` before consuming `params`: comma-separated keys, leading
    // `-` descending. Threaded into the catalog ORDER BY below.
    let sorting = parse_sort(&params);
    // Extract select_metadata before params is moved by into_iter() below.
    let select_metadata: Option<String> = params
        .iter()
        .find(|(k, _)| k == "select_metadata")
        .map(|(_, v)| v.clone());
    // ?omit_links=true drops the per-entry `links` (Python router.py:323 /
    // core.py:577). The envelope pagination links are unaffected.
    let omit_links = params
        .iter()
        .any(|(k, v)| k == "omit_links" && matches!(v.as_str(), "true" | "True" | "1"));
    // ?include_data_sources=true attaches each entry's `data_sources` list (with
    // assets) to the response, mirroring the single-node metadata route (Python
    // router.py:324 / core.py:483). Off ⇒ the field is omitted per entry.
    let include_data_sources = params
        .iter()
        .any(|(k, v)| k == "include_data_sources" && matches!(v.as_str(), "true" | "True" | "1"));
    // ?fields= projection (Python `EntryFields`). ABSENT → full entry (FastAPI
    // defaults `fields` to every EntryFields, router.py:458), so we do not prune.
    // PRESENT → each entry keeps only the requested attribute sections. The Rust
    // client sends `fields=""` (keys(): id-only, container.rs) and `fields=count`
    // (len(): the total comes from the envelope meta, not the entries).
    // `Query<Vec>` preserves repeated `fields=` keys, so a multi-section
    // projection (`fields=metadata&fields=structure`) survives intact.
    let fields: Option<Vec<String>> = {
        let vals: Vec<String> = params
            .iter()
            .filter(|(k, _)| k == "fields")
            .map(|(_, v)| v.clone())
            .collect();
        (!vals.is_empty()).then_some(vals)
    };

    let filter_params: Vec<(String, String)> = params
        .into_iter()
        .filter(|(k, _)| k.starts_with("filter["))
        .collect();
    let mut queries = crate::core::queries::decode_query_filters(&filter_params)?;

    // Per-ancestor auth gate on the parent container path.
    // Returns 404 (not 403) when any ancestor's per-node policy drops
    // ReadMetadata — same behaviour as the metadata read gate.
    let auth = if !segments.is_empty() {
        resolve_entry(&state, auth, &segments, crate::auth::Scope::ReadMetadata).await?
    } else {
        auth
    };

    // Inject the access-policy list filter so the SQL/in-memory path
    // only returns nodes the principal is permitted to see. A listing/search
    // needs read:metadata (parity with Python get_entry's filter_for_access
    // scopes=["read:metadata"], dependencies.py:78).
    if let Some(ref policy) = state.access_policy {
        let principal_ref = auth.principal.as_deref();
        let requested = crate::auth::ScopeSet::from_iter([crate::auth::Scope::ReadMetadata]);
        if let Some(f) = policy
            .list_filter(
                principal_ref,
                &auth.scopes,
                &requested,
                auth.authn_access_tags.as_deref(),
            )
            .await
        {
            queries.insert(0, crate::core::queries::Query::AccessBlobFilter(f));
        }
    }

    // One listing path for both backends. Resolve the container (root, or via
    // the async tree walk) and let its `search_page` apply the filters, sort
    // and `[offset, offset+limit)` window: SQL pushdown for the catalog,
    // in-memory screening for the rest. The catalog used to take a separate
    // direct-SQL branch here; routing both through the trait deletes that
    // divergence, so the catalog's full SQL query matrix
    // (`Comparison`/`In`/`Regex`/…) now reaches every search instead of only
    // the in-memory subset. A non-container target is rejected via the same
    // `as_container` gate the leaf endpoints use (the walk resolves it first,
    // consistent with the rest of the post-M4 read path).
    let logical_path = segments.join("/");
    let walked;
    let container: &dyn ContainerAdapter = if segments.is_empty() {
        state.root_tree.as_ref()
    } else {
        walked = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
        walked.as_container().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not a container", segments.join("/")))
        })?
    };
    // An unsupported query variant propagates as HTTP 400.
    let mut resp = core::construct_entries_response(
        container,
        &logical_path,
        &base_url,
        cursor,
        offset,
        limit,
        &queries,
        &sorting,
        state.exact_count_limit,
        include_data_sources,
    )
    .await?;
    // `select_metadata` only applies within `metadata in fields` (core.py:479-485):
    // when the projection excludes `metadata`, the expression is never evaluated,
    // so a malformed one cannot 400 a request that wasn't asking for metadata.
    let metadata_in_fields = fields
        .as_ref()
        .is_none_or(|f| f.iter().any(|r| r == "metadata"));
    if metadata_in_fields
        && let Some(ref expr_str) = select_metadata
        && let Some(ref mut items) = resp.data
    {
        for item in items.iter_mut() {
            item.attributes.metadata =
                apply_select_metadata(Some(expr_str), item.attributes.metadata.take())?;
        }
    }
    // Apply the `?fields=` projection last so it strips any section the
    // select_metadata step populated but the client did not request. Links are
    // untouched here — an id-only resource keeps its `self` link (core.py:248);
    // `omit_links` below is the only switch that drops per-entry links.
    if let Some(ref requested) = fields
        && let Some(ref mut items) = resp.data
    {
        for item in items.iter_mut() {
            prune_entry_fields(&mut item.attributes, requested);
        }
    }
    // Drop per-entry links when requested; the envelope pagination links remain.
    if omit_links && let Some(ref mut items) = resp.data {
        for item in items.iter_mut() {
            item.links = crate::core::schemas::NodeLinks::default();
        }
    }
    Ok(Json(resp))
}

// ---------------------------------------------------------------------------
// GET /api/v1/distinct/{path} — unique metadata-key values / structure
// families / specs among a container's children. Python parity:
// router.py:401-447. Catalog-only capability; a server without a catalog has
// no node that supports get_distinct → 405.
// ---------------------------------------------------------------------------

pub async fn distinct_root(
    state: State<AppState>,
    params: Query<Vec<(String, String)>>,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    distinct(
        state,
        OriginalUri("/api/v1/distinct/".parse().expect("static URI")),
        params,
        auth,
    )
    .await
}

pub async fn distinct(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<Vec<(String, String)>>,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadMetadata)?;
    let segments = segments_from_uri(&uri, "/api/v1/distinct/");

    // Facet flags + the metadata keys to inspect. httpx encodes Python bools as
    // "true"/"false"; accept "1" too. `metadata` is repeated (one per key),
    // mirroring the client's `params={"metadata": metadata_keys, ...}`
    // (client/container.py:596-602).
    let want_bool = |name: &str| {
        params
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| matches!(v.to_ascii_lowercase().as_str(), "true" | "1"))
            .unwrap_or(false)
    };
    let structure_families = want_bool("structure_families");
    let specs = want_bool("specs");
    let counts = want_bool("counts");
    let metadata_keys: Vec<String> = params
        .iter()
        .filter(|(k, _)| k == "metadata")
        .map(|(_, v)| v.clone())
        .collect();

    let filter_params: Vec<(String, String)> = params
        .iter()
        .filter(|(k, _)| k.starts_with("filter["))
        .cloned()
        .collect();
    let mut queries = crate::core::queries::decode_query_filters(&filter_params)?;

    // Per-ancestor auth gate, identical to search/metadata reads (404, not 403,
    // when an ancestor's per-node policy drops ReadMetadata).
    let auth = if !segments.is_empty() {
        resolve_entry(&state, auth, &segments, crate::auth::Scope::ReadMetadata).await?
    } else {
        auth
    };

    // Scope to the nodes the principal may see — the same list filter the
    // search path injects, so distinct counts only permitted children.
    if let Some(ref policy) = state.access_policy {
        let principal_ref = auth.principal.as_deref();
        let requested = crate::auth::ScopeSet::from_iter([crate::auth::Scope::ReadMetadata]);
        if let Some(f) = policy
            .list_filter(
                principal_ref,
                &auth.scopes,
                &requested,
                auth.authn_access_tags.as_deref(),
            )
            .await
        {
            queries.insert(0, crate::core::queries::Query::AccessBlobFilter(f));
        }
    }

    // Distinct is a catalog capability; without one, no node supports it → 405
    // (Python router.py:444-447).
    let Some(ref catalog) = state.catalog else {
        return Err(ServerError::MethodNotAllowed(
            "This node does not support distinct.".into(),
        ));
    };

    // Resolve the container's node id; root (empty path) → distinct over the
    // top-level children (parent_id IS NULL).
    let parent_id = if segments.is_empty() {
        None
    } else {
        let node = catalog
            .lookup(&segments)
            .await
            .map_err(map_catalog_err)?
            .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;
        Some(node.id)
    };

    let resp = catalog
        .get_distinct(
            parent_id,
            &queries,
            &metadata_keys,
            structure_families,
            specs,
            counts,
        )
        .await
        .map_err(map_catalog_err)?;
    Ok(Json(resp))
}

// Enforce the configured `response_bytesize_limit` (parity gap L4). Mirrors
// Python tiled, which compares the decoded data size against
// `settings.response_bytesize_limit` BEFORE packing and raises
// HTTP 400 on exceed (router.py:621/701/1185/1315/...). `nbytes` is the
// decoded in-memory size of the payload about to be serialized.
// `hint` is a family-specific suffix appended after the fixed prefix
// (array: router.py:626; table: router.py:1320).
fn check_response_size(nbytes: usize, limit: usize, hint: &str) -> Result<(), ServerError> {
    if nbytes > limit {
        return Err(ServerError::ResponseTooLarge(format!(
            "Response would exceed {limit}. {hint}"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared array response builder (used by array_block and array_full)
// ---------------------------------------------------------------------------

/// Build the HTTP 406 returned when a client's requested `?format=`/`Accept`
/// resolves to a media type this structure `family` cannot serialize. Mirrors
/// Python tiled's `UnsupportedMediaTypes` → `HTTP_406_NOT_ACCEPTABLE`
/// (router.py:642-643, core.py:374-419). The data handlers must never fall
/// back to serving the raw payload under the requested (foreign) Content-Type:
/// that silently corrupts `client.export()` (HTTP 200 with mislabeled bytes).
fn unsupported_media_type(
    family: crate::core::structures::StructureFamily,
    requested: &str,
    registry: &crate::serialization::SerializationRegistry,
) -> ServerError {
    let mut supported = registry.media_types(family);
    supported.sort();
    ServerError::NotAcceptable(format!(
        "Cannot serialize {family} as {requested:?}. Supported media types: {}.",
        supported.join(", ")
    ))
}

/// Map a serializer-execution error to an HTTP status, mirroring Python tiled:
/// an `UnsupportedShape` (data shape incompatible with the requested format,
/// e.g. a >2-D array as CSV) → 406; any other packing/I-O failure → 500
/// (core.py:441-449). Single owner for all three data-handler serializer sites.
fn map_serialize_error(e: crate::serialization::SerializeError) -> ServerError {
    if let Some(shape) = e.downcast_ref::<crate::serialization::UnsupportedShape>() {
        ServerError::NotAcceptable(format!(
            "The shape of this data {:?} is incompatible with the requested format. \
             Slice it (\"?slice=...\") or choose a different format.",
            shape.shape
        ))
    } else {
        ServerError::Internal(e.to_string())
    }
}

async fn build_array_response(
    data: crate::core::dtype::DynNDArray,
    format_param: Option<&str>,
    headers: &HeaderMap,
    state: &AppState,
    filename: Option<&str>,
) -> Result<axum::response::Response, ServerError> {
    // Cap the decoded array size before serialization (Python: array.nbytes).
    check_response_size(
        data.data.len(),
        state.response_bytesize_limit,
        "Use slicing (\"?slice=...\") to request smaller chunks.",
    )?;

    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream");

    let family = crate::core::structures::StructureFamily::Array;
    let media_type = crate::serialization::negotiate_media_type(
        format_param,
        accept,
        family,
        &state.serialization_registry,
    )
    .ok_or_else(|| {
        // `None` means the requested representation — an explicit `?format=` or
        // a concrete `Accept` header — resolves to nothing this family can
        // serve. Python raises UnsupportedMediaTypes → HTTP 406.
        unsupported_media_type(
            family,
            format_param.unwrap_or(accept),
            &state.serialization_registry,
        )
    })?;

    // Never serve the raw payload under a foreign Content-Type: if the negotiated
    // media type has no serializer for this family, error (HTTP 406) like the
    // container handler (router.rs ~:1320), instead of mislabeling raw bytes.
    let serializer = state
        .serialization_registry
        .dispatch(family, &media_type)
        .ok_or_else(|| {
            unsupported_media_type(family, &media_type, &state.serialization_registry)
        })?;

    let ser_meta = serde_json::json!({
        "itemsize": data.dtype.element_size(),
        "kind": String::from(data.dtype.kind.to_numpy_char()),
        "byteorder": String::from(data.dtype.endianness.to_numpy_char()),
        // datetime64/timedelta64 unit, e.g. "[ns]"; needed by the JSON
        // serializer to decode datetime64 ticks into ISO-8601 strings.
        "dt_units": data.dtype.dt_units,
        "shape": data.shape,
    });
    // Serializers run CPU-bound encode work (and, for HDF5, blocking file
    // I/O); offload off the async executor so a large export can't stall
    // the runtime. `dispatch` returns an Arc<SerializerFn> (Send + 'static).
    let payload = data.data;
    let body = tokio::task::spawn_blocking(move || serializer(&payload, &ser_meta))
        .await
        .map_err(|e| ServerError::Internal(format!("serialize task failed: {e}")))?
        .map_err(map_serialize_error)?;

    Ok(serve_with_range(headers, &media_type, body, filename))
}

// Shared by `table_partition` and `table_full`: encode an already-read
// `ArrowTable` to Arrow IPC and route it through the serialization registry so
// format negotiation applies (e.g. csv/parquet re-encode the IPC bytes).
// `metadata` is the table node's user metadata, handed to the serializer; only
// the HDF5 serializer reads it (→ file attrs, Python `file.attrs.update`), every
// other table serializer ignores its meta argument.
async fn build_table_response(
    table: crate::core::dtype::ArrowTable,
    metadata: serde_json::Value,
    format_param: Option<&str>,
    headers: &HeaderMap,
    state: &AppState,
    filename: Option<&str>,
) -> Result<axum::response::Response, ServerError> {
    // Cap the decoded table size before serialization (Python:
    // df.memory_usage().sum()).
    let nbytes: usize = table
        .batches
        .iter()
        .map(|b| b.get_array_memory_size())
        .sum();
    check_response_size(
        nbytes,
        state.response_bytesize_limit,
        "Select a subset of the columns to request a smaller chunk.",
    )?;

    let accept = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let family = crate::core::structures::StructureFamily::Table;
    let media_type = crate::serialization::negotiate_media_type(
        format_param,
        accept,
        family,
        &state.serialization_registry,
    )
    .ok_or_else(|| {
        // `None` — an explicit `?format=` or a concrete `Accept` that resolves
        // to nothing this family serves (Python UnsupportedMediaTypes → 406).
        // Bail before the (expensive) IPC encode.
        unsupported_media_type(
            family,
            format_param.unwrap_or(accept),
            &state.serialization_registry,
        )
    })?;

    // Arrow IPC encode is CPU-bound with no inner async — offload it.
    let ipc_bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, ServerError> {
        let mut buf = Vec::new();
        {
            let mut writer = arrow::ipc::writer::FileWriter::try_new(&mut buf, &table.schema)
                .map_err(|e| ServerError::Internal(format!("Arrow IPC write error: {e}")))?;
            for batch in &table.batches {
                writer
                    .write(batch)
                    .map_err(|e| ServerError::Internal(format!("Arrow IPC write error: {e}")))?;
            }
            writer
                .finish()
                .map_err(|e| ServerError::Internal(format!("Arrow IPC write error: {e}")))?;
        }
        Ok(buf)
    })
    .await
    .map_err(|e| ServerError::Internal(format!("blocking task failed: {e}")))??;

    // Route the Arrow IPC bytes through the serialization registry so format
    // negotiation applies (e.g., parquet/csv re-encode the IPC bytes). The
    // default `application/vnd.apache.arrow.file` is registered as an identity
    // serializer, so the only way `dispatch` misses is a `?format=` resolving
    // to a non-table media type — error (HTTP 406), never mislabel raw IPC.
    let serializer = state
        .serialization_registry
        .dispatch(family, &media_type)
        .ok_or_else(|| {
            unsupported_media_type(family, &media_type, &state.serialization_registry)
        })?;
    let body = tokio::task::spawn_blocking(move || serializer(&ipc_bytes, &metadata))
        .await
        .map_err(|e| ServerError::Internal(format!("serialize task failed: {e}")))?
        .map_err(map_serialize_error)?;

    Ok(serve_with_range(headers, &media_type, body, filename))
}

/// Convert a `DynNDArray` (raw little-endian element bytes + dtype) into an
/// Arrow array, dispatching on the dtype class — the inverse of the array
/// adapters' `to_le_bytes` emission. Used to assemble the COO columns of a
/// sparse response.
fn dyn_ndarray_to_arrow(
    arr: &crate::core::dtype::DynNDArray,
) -> Result<arrow::array::ArrayRef, ServerError> {
    use crate::core::dtype::Kind;
    use arrow::array::{
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    let bytes: &[u8] = &arr.data;
    macro_rules! build {
        ($t:ty, $arrow:ty) => {{
            const ES: usize = std::mem::size_of::<$t>();
            let vals: Vec<$t> = bytes
                .chunks_exact(ES)
                .map(|c| <$t>::from_le_bytes(c.try_into().expect("chunks_exact yields ES bytes")))
                .collect();
            Ok(Arc::new(<$arrow>::from(vals)) as arrow::array::ArrayRef)
        }};
    }
    match (arr.dtype.kind, arr.dtype.element_size()) {
        (Kind::Float, 8) => build!(f64, Float64Array),
        (Kind::Float, 4) => build!(f32, Float32Array),
        (Kind::Integer, 8) => build!(i64, Int64Array),
        (Kind::Integer, 4) => build!(i32, Int32Array),
        (Kind::Integer, 2) => build!(i16, Int16Array),
        (Kind::Integer, 1) => build!(i8, Int8Array),
        (Kind::UnsignedInteger, 8) => build!(u64, UInt64Array),
        (Kind::UnsignedInteger, 4) => build!(u32, UInt32Array),
        (Kind::UnsignedInteger, 2) => build!(u16, UInt16Array),
        (Kind::UnsignedInteger, 1) => build!(u8, UInt8Array),
        (kind, size) => Err(ServerError::Internal(format!(
            "sparse: unsupported element dtype {kind:?} of {size} bytes"
        ))),
    }
}

/// Deserialize an Arrow IPC file carrying a COO sparse table into a `SparseData`
/// — the inverse of [`build_sparse_response`]'s encode, and the server side of
/// Python `client/sparse.py::write`/`write_block` (client/sparse.py:107), which
/// builds a DataFrame of columns `dim0`…`dim{ndim-1}` (integer indices) plus
/// `data` (the non-zero values) and PUTs it as Arrow IPC. Coordinate columns are
/// normalized to int64-LE — the on-disk parquet coord type
/// (serialization/sparse_blocks_parquet.py:26) the write face reads back via
/// `coord_dyn_to_i64_vec` — while the `data` column keeps its native width
/// (Float64/Float32/Int64/Int32, the set the sparse parquet writer supports).
fn deserialize_sparse_coo(
    body: &[u8],
    ndim: usize,
) -> Result<crate::core::adapters::SparseData, ServerError> {
    use crate::core::dtype::{BuiltinDType, DynNDArray, Endianness, Kind};

    let cursor = std::io::Cursor::new(body.to_vec());
    let reader = arrow::ipc::reader::FileReader::try_new(cursor, None).map_err(|e| {
        ServerError::Validation(format!(
            "sparse write: body is not a valid Arrow IPC file: {e}"
        ))
    })?;

    // One growing i64 index vector per axis, plus the value column's raw
    // little-endian bytes and its (batch-invariant) dtype.
    let mut coord_i64: Vec<Vec<i64>> = (0..ndim).map(|_| Vec::new()).collect();
    let mut data_bytes: Vec<u8> = Vec::new();
    let mut data_dtype: Option<BuiltinDType> = None;

    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            ServerError::Validation(format!("sparse write: Arrow batch decode error: {e}"))
        })?;
        for (i, buf) in coord_i64.iter_mut().enumerate() {
            let name = format!("dim{i}");
            let col = batch.column_by_name(&name).ok_or_else(|| {
                ServerError::Validation(format!("sparse write: missing column '{name}'"))
            })?;
            buf.extend(sparse_coord_col_to_i64(col.as_ref(), &name)?);
        }
        let data_col = batch
            .column_by_name("data")
            .ok_or_else(|| ServerError::Validation("sparse write: missing column 'data'".into()))?;
        let (dtype, mut bytes) = sparse_data_col_to_le_bytes(data_col.as_ref())?;
        match &data_dtype {
            Some(prev) if *prev != dtype => {
                return Err(ServerError::Validation(
                    "sparse write: 'data' column dtype changed between Arrow batches".into(),
                ));
            }
            _ => data_dtype = Some(dtype),
        }
        data_bytes.append(&mut bytes);
    }

    let data_dtype = data_dtype.ok_or_else(|| {
        ServerError::Validation("sparse write: Arrow IPC body has no record batches".into())
    })?;
    let nnz = data_bytes.len() / data_dtype.element_size();

    let i64_dtype = BuiltinDType::new(Endianness::Little, Kind::Integer, 8);
    let coords: Vec<DynNDArray> = coord_i64
        .into_iter()
        .map(|c| {
            let bytes: Vec<u8> = c.iter().flat_map(|v| v.to_le_bytes()).collect();
            DynNDArray::new(bytes::Bytes::from(bytes), i64_dtype.clone(), vec![nnz])
        })
        .collect();
    let data = DynNDArray::new(bytes::Bytes::from(data_bytes), data_dtype, vec![nnz]);
    Ok(crate::core::adapters::SparseData { coords, data })
}

/// Reject a sparse write whose value column dtype differs from the node's
/// declared `data_type`.
///
/// The read path labels the returned values with the *stored* parquet column
/// dtype (`SparseBlocksParquetAdapter::to_sparse_data`), matching upstream's
/// pandas read (`sparse_blocks_parquet.py:31,123-127`), so a stored/declared
/// mismatch no longer corrupts a GET. This guard keeps a node's declared
/// `data_type` truthful about what its blocks actually store for every write
/// through our server: it rejects, at the raw Arrow PUT boundary, a write whose
/// value dtype would leave the node's structure metadata disagreeing with its
/// stored data. Externally-registered parquet files bypass this boundary; the
/// read path handles their stored dtype directly. The typed client always sends
/// the declared dtype, so it is unaffected.
fn ensure_sparse_data_dtype(
    structure: &crate::core::structures::SparseStructure,
    data: &crate::core::adapters::SparseData,
) -> Result<(), ServerError> {
    if let Some(crate::core::dtype::DType::Builtin(declared)) = &structure.data_type
        && *declared != data.data.dtype
    {
        return Err(ServerError::Validation(format!(
            "sparse write: value dtype {:?} does not match the node's declared \
             data_type {:?}; the node's structure metadata would then disagree \
             with its stored data",
            data.data.dtype, declared
        )));
    }
    Ok(())
}

/// Read a COO coordinate column (any integer width) as `i64`, the on-disk coord
/// type. Mirrors the client-side `col_to_i64` so a GET→PUT round-trip of the
/// same table is lossless.
fn sparse_coord_col_to_i64(
    col: &dyn arrow::array::Array,
    name: &str,
) -> Result<Vec<i64>, ServerError> {
    use arrow::array::{
        Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    };
    use arrow::datatypes::DataType;
    macro_rules! ints {
        ($arr:ty) => {
            col.as_any()
                .downcast_ref::<$arr>()
                .unwrap()
                .values()
                .iter()
                .map(|&v| v as i64)
                .collect()
        };
    }
    let out: Vec<i64> = match col.data_type() {
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec(),
        DataType::Int32 => ints!(Int32Array),
        DataType::Int16 => ints!(Int16Array),
        DataType::Int8 => ints!(Int8Array),
        DataType::UInt64 => ints!(UInt64Array),
        DataType::UInt32 => ints!(UInt32Array),
        DataType::UInt16 => ints!(UInt16Array),
        DataType::UInt8 => ints!(UInt8Array),
        other => {
            return Err(ServerError::Validation(format!(
                "sparse write: column '{name}' has non-integer type {other:?} for COO coordinates"
            )));
        }
    };
    Ok(out)
}

/// Read the COO `data` column as raw little-endian element bytes, preserving its
/// native dtype. Restricted to the value dtypes the sparse parquet writer stores
/// (Float64/Float32/Int64/Int32) so a rejected type fails here at the boundary
/// rather than deep in the encoder.
fn sparse_data_col_to_le_bytes(
    col: &dyn arrow::array::Array,
) -> Result<(crate::core::dtype::BuiltinDType, Vec<u8>), ServerError> {
    use crate::core::dtype::{BuiltinDType, Endianness, Kind};
    use arrow::array::{Float32Array, Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::DataType;
    macro_rules! le {
        ($arr:ty, $kind:expr, $size:expr) => {{
            let bytes: Vec<u8> = col
                .as_any()
                .downcast_ref::<$arr>()
                .unwrap()
                .values()
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect();
            Ok((BuiltinDType::new(Endianness::Little, $kind, $size), bytes))
        }};
    }
    match col.data_type() {
        DataType::Float64 => le!(Float64Array, Kind::Float, 8),
        DataType::Float32 => le!(Float32Array, Kind::Float, 4),
        DataType::Int64 => le!(Int64Array, Kind::Integer, 8),
        DataType::Int32 => le!(Int32Array, Kind::Integer, 4),
        other => Err(ServerError::Validation(format!(
            "sparse write: 'data' column has unsupported type {other:?} \
             (supported: float64, float32, int64, int32)"
        ))),
    }
}

// Shared by the sparse branches of `array_block` and `array_full`: encode a
// `SparseData` (COO coordinates + values) as a table with columns
// `dim0..dim{ndim-1}` and `data`, then route it through the serialization
// registry under the `Sparse` family — mirroring Python `serialization/sparse.py`
// (`to_dataframe`: one `dim{i}` column per coordinate axis plus `data`). The
// default `application/vnd.apache.arrow.file` serializer emits the Arrow IPC
// bytes verbatim; `?format=`/`Accept` can re-encode to CSV/parquet/etc.
async fn build_sparse_response(
    sparse: crate::core::adapters::SparseData,
    format_param: Option<&str>,
    headers: &HeaderMap,
    state: &AppState,
    filename: Option<&str>,
) -> Result<axum::response::Response, ServerError> {
    // Cap the decoded size before serialization: coords (ndim * nnz) + data.
    let nbytes: usize =
        sparse.coords.iter().map(|c| c.data.len()).sum::<usize>() + sparse.data.data.len();
    check_response_size(
        nbytes,
        state.response_bytesize_limit,
        "Use slicing (\"?slice=...\") to request a smaller selection.",
    )?;

    // One `dim{i}` column per coordinate axis, then the `data` column.
    let mut fields: Vec<arrow::datatypes::Field> = Vec::with_capacity(sparse.coords.len() + 1);
    let mut columns: Vec<arrow::array::ArrayRef> = Vec::with_capacity(sparse.coords.len() + 1);
    for (i, coord) in sparse.coords.iter().enumerate() {
        let col = dyn_ndarray_to_arrow(coord)?;
        fields.push(arrow::datatypes::Field::new(
            format!("dim{i}"),
            col.data_type().clone(),
            false,
        ));
        columns.push(col);
    }
    let data_col = dyn_ndarray_to_arrow(&sparse.data)?;
    fields.push(arrow::datatypes::Field::new(
        "data",
        data_col.data_type().clone(),
        false,
    ));
    columns.push(data_col);

    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
    let batch = arrow::array::RecordBatch::try_new(Arc::clone(&schema), columns)
        .map_err(|e| ServerError::Internal(format!("sparse RecordBatch error: {e}")))?;

    let family = crate::core::structures::StructureFamily::Sparse;
    let accept = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let media_type = crate::serialization::negotiate_media_type(
        format_param,
        accept,
        family,
        &state.serialization_registry,
    )
    .ok_or_else(|| {
        unsupported_media_type(
            family,
            format_param.unwrap_or(accept),
            &state.serialization_registry,
        )
    })?;

    // Arrow IPC encode is CPU-bound with no inner async — offload it.
    let ipc_bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, ServerError> {
        let mut buf = Vec::new();
        {
            let mut writer = arrow::ipc::writer::FileWriter::try_new(&mut buf, &schema)
                .map_err(|e| ServerError::Internal(format!("Arrow IPC write error: {e}")))?;
            writer
                .write(&batch)
                .map_err(|e| ServerError::Internal(format!("Arrow IPC write error: {e}")))?;
            writer
                .finish()
                .map_err(|e| ServerError::Internal(format!("Arrow IPC write error: {e}")))?;
        }
        Ok(buf)
    })
    .await
    .map_err(|e| ServerError::Internal(format!("blocking task failed: {e}")))??;

    let serializer = state
        .serialization_registry
        .dispatch(family, &media_type)
        .ok_or_else(|| {
            unsupported_media_type(family, &media_type, &state.serialization_registry)
        })?;
    let body =
        tokio::task::spawn_blocking(move || serializer(&ipc_bytes, &serde_json::Value::Null))
            .await
            .map_err(|e| ServerError::Internal(format!("serialize task failed: {e}")))?
            .map_err(map_serialize_error)?;

    Ok(serve_with_range(headers, &media_type, body, filename))
}

// Shared by `ragged_full`: encode an already-read `RaggedData` through the
// serialization registry so format negotiation applies (JSON list-of-lists,
// Awkward zip-of-buffers, and — feature-gated — Arrow IPC / Parquet). Mirrors
// Python `construct_data_response` for the ragged family (router.py:890-902).
async fn build_ragged_response(
    data: crate::core::adapters::RaggedData,
    format_param: Option<&str>,
    headers: &HeaderMap,
    state: &AppState,
    filename: Option<&str>,
) -> Result<axum::response::Response, ServerError> {
    // Cap the decoded size before serialization. Python guards on
    // `array._impl.nbytes` (the Awkward buffers' total size, router.py:882); the
    // dominant term is the data buffer = leaf-element count × itemsize.
    // `RaggedStructure.size` is exactly that leaf count; the O(rows) offset
    // buffers Python additionally counts do not change the order of magnitude.
    let nbytes = data
        .structure
        .size
        .saturating_mul(data.structure.data_type.element_size());
    check_response_size(
        nbytes,
        state.response_bytesize_limit,
        "Use slicing (\"?slice=...\") to request smaller chunks.",
    )?;

    let family = crate::core::structures::StructureFamily::Ragged;
    let accept = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let media_type = crate::serialization::negotiate_media_type(
        format_param,
        accept,
        family,
        &state.serialization_registry,
    )
    .ok_or_else(|| {
        unsupported_media_type(
            family,
            format_param.unwrap_or(accept),
            &state.serialization_registry,
        )
    })?;

    let serializer = state
        .serialization_registry
        .dispatch(family, &media_type)
        .ok_or_else(|| {
            unsupported_media_type(family, &media_type, &state.serialization_registry)
        })?;

    // The ragged serializers consume the JSON list-of-lists as their `&[u8]`
    // data argument and the serialized `RaggedStructure` as metadata (the ZIP
    // and Arrow/Parquet serializers read shape/dtype from it).
    let payload = data
        .to_json_bytes()
        .map_err(|e| ServerError::Internal(format!("ragged JSON encode failed: {e}")))?;
    let ser_meta = data
        .structure_as_metadata()
        .map_err(|e| ServerError::Internal(format!("ragged structure encode failed: {e}")))?;

    // Serializers run CPU-bound encode work; offload off the async executor.
    let body = tokio::task::spawn_blocking(move || serializer(&payload, &ser_meta))
        .await
        .map_err(|e| ServerError::Internal(format!("serialize task failed: {e}")))?
        .map_err(map_serialize_error)?;

    Ok(serve_with_range(headers, &media_type, body, filename))
}

// ---------------------------------------------------------------------------
// GET /api/v1/array/block/{*path}
// ---------------------------------------------------------------------------

pub async fn array_block(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/array/block/");
    // H2: per-node policy check.
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::ReadData,
    )
    .await?;

    let block_str = params
        .get("block")
        .map(|s| s.to_string())
        .unwrap_or_default();
    let slice_str = params
        .get("slice")
        .map(|s| s.to_string())
        .unwrap_or_default();
    let format_str = params.get("format").map(|s| s.to_string());
    let filename_str = params.get("filename").map(|s| s.to_string());
    // The async tree walk resolves each hop on the executor (a blocking
    // backend offloads its own sync work internally). It hands back an owned
    // `Arc` clone of the leaf; the read itself is a `Send` future that
    // offloads its own blocking, so it is awaited on the executor below.
    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;

    // Sparse leaf: serve the COO table (Python serialization/sparse.py). The
    // `SparseAdapterRead::read_block` contract takes only a block index (no
    // slice), so `?slice=` is honored on `/array/full`, not on a block read.
    if let Some(sparse) = adapter.as_sparse_arc() {
        let ndim = sparse.structure().shape.len();
        let block: Vec<usize> = if block_str.is_empty() {
            vec![0; ndim]
        } else {
            block_str
                .split(',')
                .map(|s| {
                    s.trim().parse::<usize>().map_err(|_| {
                        ServerError::Validation(format!(
                            "sparse block index must be a non-negative integer, got '{s}'"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        let data = sparse.read_block(&block).await.map_err(ServerError::from)?;
        return build_sparse_response(
            data,
            format_str.as_deref(),
            &headers,
            &state,
            filename_str.as_deref(),
        )
        .await;
    }

    let array_adapter: Arc<dyn crate::core::adapters::ArrayAdapterRead> =
        adapter.as_array_arc().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not an array", segments.join("/")))
        })?;

    let block_specs: Vec<BlockSpec> = if block_str.is_empty() {
        vec![BlockSpec::Single(0); array_adapter.structure().ndim()]
    } else {
        block_str
            .split(',')
            .map(|s| BlockSpec::parse(s.trim()))
            .collect::<Result<Vec<_>, _>>()?
    };
    // Honor the `slice` query parameter (numpy-style). Empty / missing →
    // full block.
    let slice = match slice_str.as_str() {
        "" => crate::core::ndslice::NDSlice::empty(),
        s => crate::core::ndslice::NDSlice::from_numpy_str(s)
            .map_err(|e| ServerError::Validation(format!("Invalid slice '{s}': {e}")))?,
    };
    // Single-chunk fast path = every axis is BlockSpec::Single. Mirrors
    // pre-#1302 behaviour exactly so existing callers see no change.
    let single_chunk: Option<Vec<usize>> = block_specs
        .iter()
        .map(|s| match s {
            BlockSpec::Single(i) => Some(*i),
            BlockSpec::Range(_, _) => None,
        })
        .collect();
    let data = if let Some(block) = single_chunk {
        array_adapter
            .read_block(&block, &slice)
            .await
            .map_err(ServerError::from)?
    } else {
        // Multi-chunk read — upstream tiled PR #1302. Slicing within a
        // multi-chunk block requires applying the slice to the assembled
        // buffer; restrict to "no slice" for the first port and surface a clear
        // 422 if both are combined. Slice across a chunk range is a follow-up
        // (needs a contiguous-buffer apply_slice helper).
        if !slice.is_empty() {
            return Err(ServerError::Validation(
                "?slice= combined with a multi-chunk ?block= range is not yet supported".into(),
            ));
        }
        read_block_range(array_adapter.as_ref(), &block_specs).await?
    };

    build_array_response(
        data,
        format_str.as_deref(),
        &headers,
        &state,
        filename_str.as_deref(),
    )
    .await
}

/// Build a Response that honors `Range: bytes=...` when present
/// (upstream tiled PR #762). Used by data routes that produce a full
/// byte buffer in memory — DuckDB httpfs and similar tools rely on
/// partial GETs to scan only the file slices they need.
/// `filename`, when present, is the client's `?filename=` query param
/// (Python `construct_data_response`, core.py:436-437): it sets
/// `Content-Disposition: attachment; filename="..."` so a browser downloads
/// the response under that name instead of rendering/naming it from the URL.
fn serve_with_range(
    headers: &HeaderMap,
    content_type: &str,
    body: bytes::Bytes,
    filename: Option<&str>,
) -> axum::response::Response {
    use axum::http::{HeaderName, HeaderValue, StatusCode, header};
    let total = body.len();
    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| parse_range(s, total));
    let accept_ranges = (
        HeaderName::from_static("accept-ranges"),
        HeaderValue::from_static("bytes"),
    );
    let mut resp = match range {
        Some((start, end)) if end >= start && end < total => {
            let slice = body.slice(start..=end);
            let mut resp = (
                StatusCode::PARTIAL_CONTENT,
                [
                    (
                        header::CONTENT_TYPE,
                        HeaderValue::from_str(content_type).unwrap_or_else(|_| {
                            HeaderValue::from_static("application/octet-stream")
                        }),
                    ),
                    accept_ranges,
                    (
                        header::CONTENT_RANGE,
                        HeaderValue::from_str(&format!("bytes {start}-{end}/{total}"))
                            .unwrap_or_else(|_| HeaderValue::from_static("bytes 0-0/0")),
                    ),
                ],
                slice,
            )
                .into_response();
            // axum's tuple response set CONTENT_LENGTH from the body, so
            // the slice length flows naturally; no manual override.
            resp.extensions_mut().insert(());
            resp
        }
        _ => (
            [
                (
                    header::CONTENT_TYPE,
                    HeaderValue::from_str(content_type)
                        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
                ),
                accept_ranges,
            ],
            body,
        )
            .into_response(),
    };
    if let Some(name) = filename
        && let Ok(value) = HeaderValue::from_str(&format!(
            "attachment; filename=\"{}\"",
            name.replace('"', "")
        ))
    {
        resp.headers_mut()
            .insert(header::CONTENT_DISPOSITION, value);
    }
    resp
}

/// Parse a single-range `Range: bytes=START-END` header. Multi-range
/// (`bytes=0-5,10-`) is not supported — multi-part responses are a
/// non-trivial slice of HTTP that very few tools (and not DuckDB
/// httpfs) actually use. Returns `None` on parse error or out-of-bounds.
fn parse_range(header: &str, total: usize) -> Option<(usize, usize)> {
    let raw = header.strip_prefix("bytes=")?;
    if raw.contains(',') {
        return None; // multi-range not supported
    }
    let (start_s, end_s) = raw.split_once('-')?;
    if start_s.is_empty() {
        // Suffix range: `bytes=-N` → last N bytes.
        let n: usize = end_s.parse().ok()?;
        if n == 0 || n > total {
            return None;
        }
        return Some((total - n, total - 1));
    }
    let start: usize = start_s.parse().ok()?;
    let end = if end_s.is_empty() {
        if total == 0 {
            return None;
        }
        total - 1
    } else {
        end_s.parse().ok()?
    };
    if start > end || end >= total {
        return None;
    }
    Some((start, end))
}

// ---------------------------------------------------------------------------
// PATCH /api/v1/array/full/{*path}?offset=…&shape=…&extend=…&persist=…
// ---------------------------------------------------------------------------
//
// Faithful port of Python `patch_array_full` (server/router.py:2097-2155) +
// `CatalogArrayAdapter.patch` (catalog/adapter.py:1643-1680). The body is the
// raw C-order bytes of the incoming data block (numpy `array.tobytes()`), whose
// shape is given by `?shape=`; the block is written INTO the array at `?offset=`,
// growing the shape when the slice overflows and `extend=true`. The response is
// the updated ArrayStructure JSON the client uses to refresh its cached
// structure.
//
//   - extend && !persist        → 400 (must persist to extend)
//   - node not writable         → 405 ("cannot accept array data")
//   - slice overflows, !extend  → 409 Conflict (raised by the zarr adapter)
//   - !persist                  → stream-only: return the current structure,
//                                 do not deserialize or write

/// Publish an `array-data` streaming event on the array node's OWN stream
/// (upstream `CatalogArrayAdapter._stream`, catalog/adapter.py:1642-1656; sparse
/// inherits it, :1846). No-op when the deployment has no catalog — an in-memory
/// tree has no subscribable node id. `body` is the raw write payload
/// (`media_type` its wire encoding); `shape`/`offset`/`block` describe the
/// written region. Emitting is best-effort: a lookup miss silently drops the
/// event rather than failing the write that already succeeded.
async fn stream_array_data(
    state: &AppState,
    segments: &[String],
    media_type: &str,
    shape: &[usize],
    offset: Option<&[usize]>,
    block: Option<&[usize]>,
    body: bytes::Bytes,
) {
    let Some(catalog) = state.catalog.as_ref() else {
        return;
    };
    let Ok(Some(node)) = catalog.lookup(segments).await else {
        return;
    };
    let seq = state.streaming_cache.incr_seq(node.id).await;
    state
        .streaming_cache
        .set(
            node.id,
            seq,
            crate::server::streaming_cache::StreamEvent::array_data(
                seq, media_type, shape, offset, block, body,
            ),
        )
        .await;
}

/// Publish an `array-ref` streaming event on the array node's OWN stream
/// (upstream `CatalogNodeAdapter.put_data_source`, catalog/adapter.py:973-992):
/// a metadata-only reference to a (re)registered array data source. `data_source`
/// is the request's data-source object, `patch` the optional `{shape, offset}`
/// descriptor, `shape` the registered array shape; the WS sender derives the
/// `?slice=` URI from these at send time. No payload. The caller
/// (`put_data_source`) is catalog-only and already holds the node id, so this
/// takes it directly; the streaming cache is a no-op in non-streaming builds.
async fn stream_array_ref(
    state: &AppState,
    node_id: i64,
    data_source: serde_json::Value,
    patch: Option<serde_json::Value>,
    shape: &[usize],
) {
    let seq = state.streaming_cache.incr_seq(node_id).await;
    state
        .streaming_cache
        .set(
            node_id,
            seq,
            crate::server::streaming_cache::StreamEvent::array_ref(seq, data_source, patch, shape),
        )
        .await;
}

/// Publish a `table-data` streaming event on the table node's OWN stream
/// (upstream `CatalogTableAdapter._stream`, catalog/adapter.py:1858-1871).
/// `partition` is `None` for a whole-table write, the partition index otherwise;
/// `append` distinguishes a partition append (PATCH) from a replace (PUT). Same
/// best-effort, catalog-gated contract as [`stream_array_data`].
async fn stream_table_data(
    state: &AppState,
    segments: &[String],
    media_type: &str,
    partition: Option<usize>,
    append: bool,
    body: bytes::Bytes,
) {
    let Some(catalog) = state.catalog.as_ref() else {
        return;
    };
    let Ok(Some(node)) = catalog.lookup(segments).await else {
        return;
    };
    let seq = state.streaming_cache.incr_seq(node.id).await;
    state
        .streaming_cache
        .set(
            node.id,
            seq,
            crate::server::streaming_cache::StreamEvent::table_data(
                seq, media_type, partition, append, body,
            ),
        )
        .await;
}

/// Publish a `ragged-data` streaming event on the ragged node's OWN stream
/// (upstream `CatalogRaggedAdapter._stream`, catalog/adapter.py:1770-1783; the
/// full-write path inherits `CatalogArrayAdapter.write`, which dispatches to
/// this overridden `_stream`). `shape` is the ragged structure's shape (variable
/// axes are `None`); `offset`/`block` describe the written region. Same
/// best-effort, catalog-gated contract as [`stream_array_data`].
async fn stream_ragged_data(
    state: &AppState,
    segments: &[String],
    media_type: &str,
    shape: &[Option<usize>],
    offset: Option<&[usize]>,
    block: Option<&[usize]>,
    body: bytes::Bytes,
) {
    let Some(catalog) = state.catalog.as_ref() else {
        return;
    };
    let Ok(Some(node)) = catalog.lookup(segments).await else {
        return;
    };
    let seq = state.streaming_cache.incr_seq(node.id).await;
    state
        .streaming_cache
        .set(
            node.id,
            seq,
            crate::server::streaming_cache::StreamEvent::ragged_data(
                seq, media_type, shape, offset, block, body,
            ),
        )
        .await;
}

pub async fn array_patch(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    auth: crate::server::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/array/full/");

    let extend = query_bool(&params, "extend", false);
    let persist = query_bool(&params, "persist", true);
    // Python rejects extend=true with persist=false up front (router.py:2112).
    if extend && !persist {
        return Err(ServerError::BadRequest(
            "Cannot PATCH an array with both parameters extend=true and \
             persist=false. To extend the array, you must persist the changes. \
             To skip persisting the changes, you must not extend the array."
                .into(),
        ));
    }
    // `?shape=` (incoming data block shape) and `?offset=` (where to place it)
    // are required query params (Python `shape_param` / `offset_param`).
    let shape =
        parse_csv_usize(params.get("shape").ok_or_else(|| {
            ServerError::BadRequest("array patch requires a ?shape= param".into())
        })?)?;
    let offset = parse_csv_usize(params.get("offset").ok_or_else(|| {
        ServerError::BadRequest("array patch requires an ?offset= param".into())
    })?)?;

    // Per-node policy check (matches every other data handler) → 404 on denial.
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::WriteData,
    )
    .await?;

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
    let array_adapter: Arc<dyn crate::core::adapters::ArrayAdapterRead> =
        adapter.as_array_arc().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not an array", segments.join("/")))
        })?;
    // Python returns 405 when the node has no `patch` (catalog/adapter.py exposes
    // it only for writable zarr nodes); a read-only array adapter answers 405.
    let writable = array_adapter.as_writable().ok_or_else(|| {
        ServerError::MethodNotAllowed("This node cannot accept array data.".into())
    })?;

    // Stream the incoming block BEFORE the persist branch — upstream `patch`
    // calls `_stream` before `if not persist: return` (catalog/adapter.py:
    // 1702-1706), so subscribers see the block even on a stream-only patch.
    // `shape`/`offset` are the incoming-block dimensions (`?shape=` / `?offset=`).
    stream_array_data(
        &state,
        &segments,
        crate::core::media_type::mime::OCTET_STREAM,
        &shape,
        Some(&offset),
        None,
        body.clone(),
    )
    .await;

    // !persist: stream-only. Python returns entry.structure() before
    // deserializing or writing (catalog/adapter.py:1648-1649).
    if !persist {
        let structure_json = serde_json::to_value(array_adapter.structure())
            .map_err(|e| ServerError::Internal(format!("serialize array structure: {e}")))?;
        return Ok(Json(structure_json));
    }

    // Deserialize the body into the incoming data block (raw C-order bytes in the
    // node's dtype, shape from `?shape=`). Only builtin dtypes round-trip the
    // octet-stream wire.
    let dtype = match &array_adapter.structure().data_type {
        crate::core::dtype::DType::Builtin(b) => b.clone(),
        _ => {
            return Err(ServerError::Validation(
                "array patch: only builtin (non-struct) dtypes are supported".into(),
            ));
        }
    };
    let elem = dtype.element_size();
    let nelem: usize = shape.iter().product();
    let expected = nelem * elem;
    if body.len() != expected {
        return Err(ServerError::Validation(format!(
            "array patch: body is {} bytes but shape {shape:?} of {elem}-byte \
             elements needs {expected}",
            body.len(),
        )));
    }
    let data = crate::core::dtype::DynNDArray::new(body, dtype, shape);

    let (new_shape, new_chunks) = writable
        .patch(data, &offset, extend)
        .await
        .map_err(ServerError::from)?;
    let persisted = persist_array_patch(&state, &segments, &new_shape, &new_chunks).await?;

    // Data-appended streaming events (with payloads) are re-added in PR3–PR5 on
    // the per-node streaming cache; the transient gap is accepted (Wave-24 PR2b).
    Ok(Json(persisted))
}

/// Persist an array `patch`'s new `(shape, chunks)` to the catalog and return
/// the updated structure to send back. Faithful to Python
/// `CatalogArrayAdapter.patch` (catalog/adapter.py:1661-1680): clone the stored
/// structure row and overwrite only `shape` and `chunks`, leaving `data_type`,
/// `dims`, and `resizable` intact.
async fn persist_array_patch(
    state: &AppState,
    segments: &[String],
    new_shape: &[usize],
    new_chunks: &[Vec<usize>],
) -> Result<serde_json::Value, ServerError> {
    let catalog = state.catalog.as_ref().ok_or_else(|| {
        ServerError::Internal("array patch requires a catalog to persist the new structure".into())
    })?;
    let node = catalog
        .lookup(segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("node '{}' not found", segments.join("/"))))?;
    let data_sources = catalog
        .list_data_sources(node.id)
        .await
        .map_err(map_catalog_err)?;
    let ds = data_sources.first().ok_or_else(|| {
        ServerError::Internal(format!(
            "array node '{}' has no data source to update",
            segments.join("/")
        ))
    })?;

    let mut persisted = ds.structure.clone();
    if let Some(obj) = persisted.as_object_mut() {
        obj.insert("shape".into(), serde_json::json!(new_shape));
        obj.insert("chunks".into(), serde_json::json!(new_chunks));
    }
    catalog
        .update_data_source(ds.id, persisted.clone(), ds.parameters.clone())
        .await
        .map_err(map_catalog_err)?;
    Ok(persisted)
}

// ---------------------------------------------------------------------------
// PUT /api/v1/array/full/{*path} — overwrite a writable array's data
// ---------------------------------------------------------------------------
//
// The write counterpart of `GET /array/full`. The body is the raw C-order
// element buffer for the whole array, in the node's registered dtype/shape
// (numpy's `ndarray.tobytes()`). Only internally-managed arrays whose backing
// file lives under the server's writable storage are writable — the resolver
// decides this, so a node that is not writable answers 405 rather than the
// route silently not existing. Mirrors Python tiled's `PUT /array/full`
// (router.py write_array), scoped here to the whole-array (single-block) case
// the NPY backend supports.
pub async fn array_full_put(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    auth: crate::server::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/array/full/");
    // Per-node policy check, same as every other data handler.
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::WriteData,
    )
    .await?;

    // `persist=false` (non-default) streams the write to subscribers but skips the
    // storage commit. Upstream `write` streams via `_stream` BEFORE `if not
    // persist: return` (catalog/adapter.py:1665-1670), so both families below emit
    // the payload ahead of the persist gate. Mirrors `put_array_full`'s `persist`
    // (router.py:2022) which gates the {array, sparse} write uniformly.
    let persist = query_bool(&params, "persist", true);

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;

    // Sparse (COO) leaf: the PUT body is an Arrow IPC COO table, not a dense
    // C-order buffer, so it deserializes through `deserialize_sparse_coo` and
    // writes through the sparse write face. Mirrors upstream `put_array_full`
    // accepting both array and sparse families (router.py:2018): dense reuse of
    // the array path below would misread the Arrow bytes as a raw element buffer.
    if let Some(sparse) = adapter.as_sparse_arc() {
        let writable = sparse.as_writable().ok_or_else(|| {
            ServerError::MethodNotAllowed(
                "this sparse node is not writable; only internally-managed sparse \
                 arrays under the server's writable storage accept writes"
                    .into(),
            )
        })?;
        let ndim = sparse.structure().shape.len();
        let stream_shape = sparse.structure().shape.clone();

        // Sparse inherits the array `_stream` upstream (adapter.py:1665-1670): emit
        // an `array-data` event carrying the COO write body (Arrow IPC) BEFORE the
        // persist gate, so a stream-only write still reaches subscribers.
        stream_array_data(
            &state,
            &segments,
            crate::core::media_type::mime::ARROW_FILE,
            &stream_shape,
            None,
            None,
            body.clone(),
        )
        .await;

        if persist {
            let data = deserialize_sparse_coo(&body, ndim)?;
            ensure_sparse_data_dtype(sparse.structure(), &data)?;
            writable.write(data).await.map_err(ServerError::from)?;
        }

        return Ok(Json(serde_json::json!({ "ok": true })));
    }

    let array_adapter: Arc<dyn crate::core::adapters::ArrayAdapterRead> =
        adapter.as_array_arc().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not an array", segments.join("/")))
        })?;
    let writable = array_adapter.as_writable().ok_or_else(|| {
        ServerError::MethodNotAllowed(
            "this array node is not writable; only internally-managed arrays under \
             the server's writable storage accept writes"
                .into(),
        )
    })?;

    let structure = array_adapter.structure();
    let shape = structure.shape.clone();

    // Stream the whole-array write BEFORE the persist gate — upstream `write`
    // calls `_stream` ahead of `if not persist: return` (catalog/adapter.py:
    // 1665-1670), so a stream-only write still reaches subscribers. The dense wire
    // encoding is a raw C-order buffer (octet-stream).
    stream_array_data(
        &state,
        &segments,
        crate::core::media_type::mime::OCTET_STREAM,
        &shape,
        None,
        None,
        body.clone(),
    )
    .await;

    if persist {
        let dtype = match &structure.data_type {
            crate::core::dtype::DType::Builtin(b) => b.clone(),
            _ => {
                return Err(ServerError::Validation(
                    "array write: only builtin (non-struct) dtypes are supported".into(),
                ));
            }
        };
        let elem = dtype.element_size();
        let nelem: usize = structure.shape.iter().product();
        let expected = nelem * elem;
        if body.len() != expected {
            return Err(ServerError::Validation(format!(
                "array write: body is {} bytes but the array needs {expected} \
                 (shape {:?}, {elem}-byte elements)",
                body.len(),
                structure.shape
            )));
        }
        let payload = crate::core::dtype::DynNDArray::new(body, dtype, shape);
        writable.write(payload).await.map_err(ServerError::from)?;
    }

    Ok(Json(serde_json::json!({ "ok": true })))
}

/// Parse the `?block=i,j,…` query parameter into one chunk index per axis.
/// Empty defaults to the origin chunk `[0,…]`. Shared by the array and sparse
/// PUT-block handlers: `?block=` addresses exactly one chunk (a single index per
/// axis, no ranges), so the arity must equal the node's dimensionality.
fn parse_block_param(
    params: &HashMap<String, String>,
    ndim: usize,
) -> Result<Vec<usize>, ServerError> {
    let block_str = params.get("block").map(|s| s.as_str()).unwrap_or("");
    let block: Vec<usize> = if block_str.is_empty() {
        vec![0usize; ndim]
    } else {
        block_str
            .split(',')
            .map(|s| {
                s.trim().parse::<usize>().map_err(|_| {
                    ServerError::Validation(format!(
                        "block index must be a non-negative integer, got '{s}'"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    if block.len() != ndim {
        return Err(ServerError::Validation(format!(
            "block has {} indices but the array is {ndim}-dimensional",
            block.len()
        )));
    }
    Ok(block)
}

// ---------------------------------------------------------------------------
// PUT /api/v1/array/block/{*path} — overwrite a single chunk
// ---------------------------------------------------------------------------
//
// The write counterpart of `GET /array/block`. `?block=i,j,…` addresses one
// chunk (a single index per axis — no ranges, unlike the multi-chunk read),
// and the body is that chunk's raw C-order buffer. Mirrors Python tiled's
// `PUT /array/block` (`router.py`): the partial-write path for chunked stores
// (zarr), distinct from the whole-array `PUT /array/full`.
pub async fn array_block_put(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    auth: crate::server::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/array/block/");
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::WriteData,
    )
    .await?;

    // `persist=false` (non-default) streams the block to subscribers but skips the
    // storage commit. Upstream `write_block` streams via `_stream` BEFORE `if not
    // persist: return` (catalog/adapter.py:1682-1699), so both families below emit
    // the payload ahead of the persist gate. Mirrors `put_array_block`'s `persist`
    // (router.py:2065) which gates the {array, sparse} write uniformly.
    let persist = query_bool(&params, "persist", true);

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;

    // Sparse (COO) leaf: one block's non-zeros arrive as an Arrow IPC COO table,
    // not a dense chunk buffer. Deserialize and route to the sparse write face's
    // `write_block`. Mirrors upstream `put_array_block` accepting {array, sparse}
    // (router.py); the dense chunk-shape / body-length checks below do not apply
    // to a variable-`nnz` COO block.
    if let Some(sparse) = adapter.as_sparse_arc() {
        let writable = sparse.as_writable().ok_or_else(|| {
            ServerError::MethodNotAllowed(
                "this sparse node is not writable; only internally-managed sparse \
                 arrays under the server's writable storage accept writes"
                    .into(),
            )
        })?;
        let block = parse_block_param(&params, sparse.structure().shape.len())?;
        let stream_shape = sparse.structure().shape.clone();

        // Sparse inherits the array `_stream` (adapter.py:1682-1699): emit
        // `array-data` for the block, carrying the COO body (Arrow IPC), BEFORE the
        // persist gate so a stream-only write still reaches subscribers.
        stream_array_data(
            &state,
            &segments,
            crate::core::media_type::mime::ARROW_FILE,
            &stream_shape,
            None,
            Some(&block),
            body.clone(),
        )
        .await;

        if persist {
            let data = deserialize_sparse_coo(&body, block.len())?;
            ensure_sparse_data_dtype(sparse.structure(), &data)?;
            writable
                .write_block(data, &block)
                .await
                .map_err(ServerError::from)?;
        }

        return Ok(Json(serde_json::json!({ "ok": true })));
    }

    let array_adapter: Arc<dyn crate::core::adapters::ArrayAdapterRead> =
        adapter.as_array_arc().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not an array", segments.join("/")))
        })?;
    let writable = array_adapter.as_writable().ok_or_else(|| {
        ServerError::MethodNotAllowed(
            "this array node is not writable; only internally-managed arrays under \
             the server's writable storage accept writes"
                .into(),
        )
    })?;

    let structure = array_adapter.structure();
    let ndim = structure.shape.len();

    // `?block=` is one index per axis (no ranges: a write targets exactly one
    // chunk). Empty defaults to the origin chunk.
    let block = parse_block_param(&params, ndim)?;

    // The addressed chunk's shape comes from the structure's chunk grid; the
    // body must hold exactly that many elements.
    let mut chunk_shape = Vec::with_capacity(ndim);
    for (axis, &b) in block.iter().enumerate() {
        let sizes = &structure.chunks[axis];
        let len = *sizes.get(b).ok_or_else(|| {
            ServerError::Validation(format!(
                "block index {b} out of range on axis {axis} ({} chunks)",
                sizes.len()
            ))
        })?;
        chunk_shape.push(len);
    }

    // Stream the written chunk BEFORE the persist gate — upstream `write_block`
    // calls `_stream` ahead of `if not persist: return` (catalog/adapter.py:
    // 1682-1699). `block` is the chunk coordinate, `shape` the chunk's dense shape,
    // wire encoding raw C-order.
    stream_array_data(
        &state,
        &segments,
        crate::core::media_type::mime::OCTET_STREAM,
        &chunk_shape,
        None,
        Some(&block),
        body.clone(),
    )
    .await;

    if persist {
        let dtype = match &structure.data_type {
            crate::core::dtype::DType::Builtin(b) => b.clone(),
            _ => {
                return Err(ServerError::Validation(
                    "array write: only builtin (non-struct) dtypes are supported".into(),
                ));
            }
        };
        let elem = dtype.element_size();
        let expected = chunk_shape.iter().product::<usize>() * elem;
        if body.len() != expected {
            return Err(ServerError::Validation(format!(
                "array block write: body is {} bytes but chunk {block:?} needs {expected} \
                 (chunk shape {chunk_shape:?}, {elem}-byte elements)",
                body.len()
            )));
        }
        let payload = crate::core::dtype::DynNDArray::new(body, dtype, chunk_shape);
        writable
            .write_block(payload, &block)
            .await
            .map_err(ServerError::from)?;
    }

    Ok(Json(serde_json::json!({ "ok": true })))
}

// ---------------------------------------------------------------------------
// POST /api/v1/array/full — long-URL workaround for the GET counterpart
// ---------------------------------------------------------------------------
//
// Mirrors upstream tiled PR #657. Mostly applies when slice / block /
// filter parameters get long enough to bump into the practical URL
// length cap (some intermediaries clip ~8 KB; the JSON body version
// has no such limit). Body shape: `{path, slice?, block?, format?}`.

#[derive(Debug, Deserialize)]
pub struct LongRequest {
    /// Forward-slash-separated tree path. Empty string = root.
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub slice: Option<String>,
    #[serde(default)]
    pub block: Option<String>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub filename: Option<String>,
}

impl LongRequest {
    fn to_query_params(&self) -> HashMap<String, String> {
        let mut p = HashMap::new();
        if let Some(s) = &self.slice {
            p.insert("slice".to_string(), s.clone());
        }
        if let Some(b) = &self.block {
            p.insert("block".to_string(), b.clone());
        }
        if let Some(f) = &self.format {
            p.insert("format".to_string(), f.clone());
        }
        if let Some(name) = &self.filename {
            p.insert("filename".to_string(), name.clone());
        }
        p
    }
}

pub async fn array_full_post(
    state: State<AppState>,
    BaseUrl(base_url): BaseUrl,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
    Json(req): Json<LongRequest>,
) -> Result<axum::response::Response, ServerError> {
    let _ = base_url;
    let path = req.path.trim_start_matches('/');
    let uri: axum::http::Uri = format!("/api/v1/array/full/{path}")
        .parse()
        .map_err(|e| ServerError::Internal(format!("rebuild URI: {e}")))?;
    array_full(
        state,
        OriginalUri(uri),
        Query(req.to_query_params()),
        headers,
        auth,
    )
    .await
    .map(IntoResponse::into_response)
}

pub async fn container_full_post(
    state: State<AppState>,
    BaseUrl(base_url): BaseUrl,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
    Json(req): Json<LongRequest>,
) -> Result<axum::response::Response, ServerError> {
    let path = req.path.trim_start_matches('/');
    let uri: axum::http::Uri = format!("/api/v1/container/full/{path}")
        .parse()
        .map_err(|e| ServerError::Internal(format!("rebuild URI: {e}")))?;
    container_full(
        state,
        OriginalUri(uri),
        BaseUrl(base_url),
        Query(req.to_query_params()),
        headers,
        auth,
    )
    .await
    .map(IntoResponse::into_response)
}

/// `POST /api/v1/container/full/{path}` — the xarray wide-table fallback. When a
/// dataset's variable list is long enough that repeated `field=` query params
/// would overflow the GET URI, the client moves the field list into a JSON-array
/// body (`tiled/client/xarray.py:206`, `json=variables`) and the server reads it
/// from there. Mirrors upstream `post_container_full` (router.py:1390, `field:
/// List[str] = Body(None)`) and the sibling `post_table_partition` above:
/// `format`/`filename` stay query params, the path stays in the URI, an absent or
/// empty body means "all children". Only the arrow representation is served here
/// (the sole reason to POST to this route); a non-arrow POST falls through to the
/// shared GET logic via `container_full`.
pub async fn post_container_full(
    state: State<AppState>,
    OriginalUri(uri): OriginalUri,
    BaseUrl(base_url): BaseUrl,
    Query(params): Query<Vec<(String, String)>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
    fields: Option<Json<Vec<String>>>,
) -> Result<axum::response::Response, ServerError> {
    auth.require(crate::auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/container/full/");
    let format_param = params
        .iter()
        .find(|(k, _)| k == "format")
        .map(|(_, v)| v.clone());
    let filename_param = params
        .iter()
        .find(|(k, _)| k == "filename")
        .map(|(_, v)| v.clone());
    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if wants_arrow(format_param.as_deref(), accept) {
        // H2: per-node policy check (parity with the GET path's resolve_entry).
        if !segments.is_empty() {
            let _ = resolve_entry(
                &state,
                auth.clone(),
                &segments,
                crate::auth::Scope::ReadData,
            )
            .await?;
        }
        let fields = fields.map(|Json(f)| f).filter(|f| !f.is_empty());
        return serve_container_arrow(&state, &segments, fields, filename_param, &headers).await;
    }

    // Non-arrow POST: delegate to the shared GET logic. The bare-list body is the
    // column projection (upstream `container_full(field=field)` is shared by GET
    // and POST, router.py:1428); forward it as repeated `field=` query keys so the
    // GET path resolves the projection through the same `repeated_query_values`
    // call — one projection resolution for both entry points, applied uniformly to
    // every non-arrow format. format/filename ride the `Query` map as before.
    let mut query: HashMap<String, String> = HashMap::new();
    if let Some(f) = format_param {
        query.insert("format".to_string(), f);
    }
    if let Some(name) = filename_param {
        query.insert("filename".to_string(), name);
    }
    let body_fields = fields.map(|Json(f)| f).filter(|f| !f.is_empty());
    let uri = match body_fields {
        Some(fields) => append_field_query(&uri, &fields)?,
        None => uri,
    };
    container_full(
        state,
        OriginalUri(uri),
        BaseUrl(base_url),
        Query(query),
        headers,
        auth,
    )
    .await
    .map(IntoResponse::into_response)
}

/// Whether the caller requested the Apache Arrow representation, honouring the
/// same `?format=` hard priority as [`crate::serialization::negotiate_media_type`]:
/// a `format` param settles it (the client sends the full MIME type), otherwise
/// the `Accept` header is consulted.
fn wants_arrow(format_param: Option<&str>, accept: &str) -> bool {
    if let Some(fmt) = format_param {
        return matches!(fmt, "arrow" | "feather" | "ipc")
            || fmt == crate::core::media_type::mime::ARROW_FILE;
    }
    accept.split(',').any(|part| {
        part.split(';').next().map(str::trim) == Some(crate::core::media_type::mime::ARROW_FILE)
    })
}

/// Collect the values of one or more repeated query keys off a raw URI, in order.
/// `Query<HashMap>` collapses repeated keys, so the arrow column projection
/// (`?field=a&field=b`) must be read from the query string directly. Returns
/// `None` when no such key is present (→ "all children").
fn repeated_query_values(uri: &axum::http::Uri, keys: &[&str]) -> Option<Vec<String>> {
    let query = uri.query()?;
    let vals: Vec<String> = url::form_urlencoded::parse(query.as_bytes())
        .filter(|(k, _)| keys.contains(&k.as_ref()))
        .map(|(_, v)| v.into_owned())
        .collect();
    (!vals.is_empty()).then_some(vals)
}

/// Restrict a container's top-level children to the requested column projection,
/// mirroring upstream `MapAdapter.read(fields)` (adapters/mapping.py:280-294):
/// the projected container exposes exactly `fields`, in request order, and an
/// absent field raises `KeyError` — which the shared `container_full` router
/// turns into HTTP 400 `No such field {key}.` (router.py:1442-1445). The
/// parity-fork more-precise-status rule does not apply (upstream already answers
/// 400, not 500), so we return 400 verbatim with the same detail string.
///
/// Access filtering composes on top exactly as upstream — `read(fields)` first,
/// then per-node `filter_for_access` — so a field that exists but the caller
/// cannot see is dropped silently, not rejected. `all_keys` is the container's
/// full, unfiltered child set (the validation universe, matching `self._mapping`
/// in `read`); `visible_keys` is the access-filtered set (equal to `all_keys`
/// when no access policy is in force). Returns the projected keys in field
/// order, or the first unknown field's 400.
fn apply_child_projection(
    all_keys: &[String],
    visible_keys: &[String],
    fields: &[String],
) -> Result<Vec<String>, ServerError> {
    let mut projected = Vec::with_capacity(fields.len());
    for field in fields {
        if !all_keys.iter().any(|k| k == field) {
            return Err(ServerError::BadRequest(format!("No such field {field}.")));
        }
        if visible_keys.iter().any(|k| k == field) {
            projected.push(field.clone());
        }
    }
    Ok(projected)
}

/// Append a column projection to `uri` as repeated `field=` query keys,
/// preserving the existing scheme/authority/path/query. The POST
/// `/container/full` entry point carries the projection as a bare-list body;
/// forwarding it this way lets the shared GET logic resolve it via
/// [`repeated_query_values`], so both entry points share ONE projection-
/// resolution path (upstream `container_full(field=field)` is likewise shared by
/// GET and POST, router.py:1376/1421).
fn append_field_query(
    uri: &axum::http::Uri,
    fields: &[String],
) -> Result<axum::http::Uri, ServerError> {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for f in fields {
        serializer.append_pair("field", f);
    }
    let encoded = serializer.finish();
    let path = uri.path();
    let path_and_query = match uri.query() {
        Some(q) if !q.is_empty() => format!("{path}?{q}&{encoded}"),
        _ => format!("{path}?{encoded}"),
    };
    let path_and_query: axum::http::uri::PathAndQuery = path_and_query
        .parse()
        .map_err(|e| ServerError::Internal(format!("uri rebuild: {e}")))?;
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(path_and_query);
    axum::http::Uri::from_parts(parts)
        .map_err(|e| ServerError::Internal(format!("uri rebuild: {e}")))
}

/// Serialize an `xarray_dataset` container's array children as a single Arrow IPC
/// FILE — one column per variable. This mirrors upstream `serialize_dataset_arrow`
/// (serialization/xarray.py:68 → `as_dataset(node).to_dataframe()`), adapted to run
/// inline because the Rust serialization registry is keyed on `StructureFamily`,
/// not on the `xarray_dataset` spec upstream dispatches on. `fields` is the column
/// projection (a subset of child keys in request order, upstream
/// `MapAdapter.read(fields=…)`, mapping.py:280); `None` reads every child.
async fn serve_container_arrow(
    state: &AppState,
    segments: &[String],
    fields: Option<Vec<String>>,
    filename: Option<String>,
    headers: &HeaderMap,
) -> Result<axum::response::Response, ServerError> {
    let walked;
    let container: &dyn ContainerAdapter = if segments.is_empty() {
        state.root_tree.as_ref()
    } else {
        walked = core::walk_tree(state.root_tree.as_ref(), segments).await?;
        walked.as_container().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not a container", segments.join("/")))
        })?
    };

    // Parity gate: only an `xarray_dataset` container has an arrow serializer
    // upstream. A plain container → 406, exactly as the family-keyed
    // `negotiate_media_type` answers for every other unsupported format.
    if !container.specs().iter().any(|s| s.name == "xarray_dataset") {
        return Err(unsupported_media_type(
            crate::core::structures::StructureFamily::Container,
            crate::core::media_type::mime::ARROW_FILE,
            &state.serialization_registry,
        ));
    }

    let keys: Vec<String> = match fields {
        Some(f) => f,
        None => container.keys().await?,
    };

    let slice = crate::core::ndslice::NDSlice::empty();
    let mut columns: Vec<(String, crate::core::dtype::DynNDArray)> = Vec::with_capacity(keys.len());
    let mut cumulative = 0usize;
    for key in keys {
        let child = container
            .get(&key)
            .await?
            .ok_or_else(|| ServerError::NotFound(format!("no child named '{key}'")))?;
        let arr = child.as_array().ok_or_else(|| {
            ServerError::WrongType(format!(
                "child '{key}' is not an array; xarray wide-table export requires array variables"
            ))
        })?;
        // Parity with `as_dataset`: every variable must be a coord or data var.
        if !arr
            .specs()
            .iter()
            .any(|s| s.name == "xarray_coord" || s.name == "xarray_data_var")
        {
            return Err(ServerError::Validation(format!(
                "child '{key}' lacks an 'xarray_coord'/'xarray_data_var' spec; not an xarray dataset variable"
            )));
        }
        let data = arr.read(&slice).await.map_err(ServerError::from)?;
        cumulative += data.data.len();
        check_response_size(
            cumulative,
            state.response_bytesize_limit,
            "Select a subset of the data to request a smaller chunk.",
        )?;
        columns.push((key, data));
    }

    // Column packing + IPC encode is CPU-bound → run off the async executor.
    let ipc = tokio::task::spawn_blocking(move || build_container_arrow_ipc(columns))
        .await
        .map_err(|e| ServerError::Internal(format!("arrow build task failed: {e}")))??;

    Ok(serve_with_range(
        headers,
        crate::core::media_type::mime::ARROW_FILE,
        bytes::Bytes::from(ipc),
        filename.as_deref(),
    ))
}

/// Pack `(name, array)` columns into one Arrow IPC FILE record batch. Each array
/// is flattened to its element sequence and becomes one primitive column; the
/// supported dtypes mirror the client's wide-table decoder
/// (`xarray_client.rs::arrow_dtype_to_tiled_dtype`): f64/f32/i64/i32/u64/u32.
/// Columns must share length (the wide-table invariant) — `RecordBatch::try_new`
/// enforces it and a mismatch surfaces as 422, which the client treats as a
/// signal to fall back to per-array reads.
fn build_container_arrow_ipc(
    columns: Vec<(String, crate::core::dtype::DynNDArray)>,
) -> Result<Vec<u8>, ServerError> {
    use arrow::datatypes::Schema;

    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays = Vec::with_capacity(columns.len());
    for (name, arr) in &columns {
        let (field, values) = arrow_column_from_ndarray(name, arr)?;
        fields.push(field);
        arrays.push(values);
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = arrow::record_batch::RecordBatch::try_new(schema.clone(), arrays).map_err(|e| {
        ServerError::Validation(format!(
            "cannot assemble wide-table arrow batch (columns must share length): {e}"
        ))
    })?;

    let mut ipc = Vec::new();
    {
        let mut writer = arrow::ipc::writer::FileWriter::try_new(&mut ipc, &schema)
            .map_err(|e| ServerError::Internal(format!("arrow ipc: {e}")))?;
        writer
            .write(&batch)
            .map_err(|e| ServerError::Internal(format!("arrow ipc: {e}")))?;
        writer
            .finish()
            .map_err(|e| ServerError::Internal(format!("arrow ipc: {e}")))?;
    }
    Ok(ipc)
}

/// Convert one flattened [`DynNDArray`] into an Arrow field + primitive column,
/// honouring the array's declared endianness. Unsupported dtypes → 406 (the
/// client only decodes the six primitive types below).
fn arrow_column_from_ndarray(
    name: &str,
    arr: &crate::core::dtype::DynNDArray,
) -> Result<(arrow::datatypes::Field, arrow::array::ArrayRef), ServerError> {
    use crate::core::dtype::{Endianness, Kind};
    use arrow::array::{
        ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field};

    let n = arr.len();
    let itemsize = arr.dtype.itemsize;
    let expected = n.saturating_mul(itemsize);
    if arr.data.len() != expected {
        return Err(ServerError::Internal(format!(
            "variable '{name}': {} bytes but shape {:?} implies {n} elements of {itemsize}B",
            arr.data.len(),
            arr.shape
        )));
    }
    let big = matches!(arr.dtype.endianness, Endianness::Big);
    let bytes = &arr.data;

    macro_rules! decode {
        ($t:ty, $sz:literal, $ArrowArr:ty) => {{
            let mut v: Vec<$t> = Vec::with_capacity(n);
            for chunk in bytes.chunks_exact($sz) {
                let a: [u8; $sz] = chunk
                    .try_into()
                    .expect("chunks_exact yields fixed-size chunks");
                v.push(if big {
                    <$t>::from_be_bytes(a)
                } else {
                    <$t>::from_le_bytes(a)
                });
            }
            Arc::new(<$ArrowArr>::from(v)) as ArrayRef
        }};
    }

    let (dt, values): (DataType, ArrayRef) = match (arr.dtype.kind, itemsize) {
        (Kind::Float, 8) => (DataType::Float64, decode!(f64, 8, Float64Array)),
        (Kind::Float, 4) => (DataType::Float32, decode!(f32, 4, Float32Array)),
        (Kind::Integer, 8) => (DataType::Int64, decode!(i64, 8, Int64Array)),
        (Kind::Integer, 4) => (DataType::Int32, decode!(i32, 4, Int32Array)),
        (Kind::UnsignedInteger, 8) => (DataType::UInt64, decode!(u64, 8, UInt64Array)),
        (Kind::UnsignedInteger, 4) => (DataType::UInt32, decode!(u32, 4, UInt32Array)),
        (kind, sz) => {
            return Err(ServerError::NotAcceptable(format!(
                "xarray wide-table arrow export does not support dtype (kind={kind:?}, itemsize={sz}) for variable '{name}'"
            )));
        }
    };
    Ok((Field::new(name, dt, false), values))
}

// ---------------------------------------------------------------------------
// GET /api/v1/container/full/{*path} — export entire container
// ---------------------------------------------------------------------------
//
// Upstream tiled PR #660. Walks the container's immediate children and
// dispatches to whichever container serializer the Accept header asks
// for (HTML index, json-seq listing). Container-format outputs that
// concatenate child *data* into one file (HDF5, Zarr, zip-of-arrays)
// would require a dedicated serializer that walks recursively and pulls
// each leaf's bytes — call it out as a deferred follow-up.

pub async fn container_full_root(
    state: State<AppState>,
    base_url: BaseUrl,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
) -> Result<axum::response::Response, ServerError> {
    container_full(
        state,
        OriginalUri("/api/v1/container/full/".parse().expect("static URI")),
        base_url,
        Query(params),
        headers,
        auth,
    )
    .await
    .map(IntoResponse::into_response)
}

pub async fn container_full(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    BaseUrl(base_url): BaseUrl,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/container/full/");
    // H2: per-node policy check.
    if !segments.is_empty() {
        let _ = resolve_entry(
            &state,
            auth.clone(),
            &segments,
            crate::auth::Scope::ReadData,
        )
        .await?;
    }

    // No Accept header expresses no preference → resolve to the container
    // family default (text/html) via `negotiate_media_type`. Do NOT substitute a
    // concrete type here: that would make a no-Accept request indistinguishable
    // from an explicit unsupported one and defeat the 406 below.
    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let format_str = params.get("format").map(|s| s.to_string());
    let filename_str = params.get("filename").map(|s| s.to_string());

    // Wide-table arrow export. Upstream registers the Container→arrow serializer
    // under the `xarray_dataset` *spec* (serialization/xarray.py:68); the Rust
    // serialization registry keys on `StructureFamily` only, so a spec-keyed
    // serializer is unrepresentable and `negotiate_media_type` below (family-
    // keyed) would answer 406. Intercept the arrow request here and mirror the
    // serializer's logic inline in `serve_container_arrow`, which still 406s for
    // a non-`xarray_dataset` container — exactly what negotiation would do. The
    // repeated `field`/`column` query keys are the column projection (upstream
    // `field` query param, router.py:1352); `Query<HashMap>` collapses repeated
    // keys, so read them straight off the raw URI.
    if wants_arrow(format_str.as_deref(), accept) {
        let fields = repeated_query_values(&uri, &["field", "column"]);
        return serve_container_arrow(&state, &segments, fields, filename_str, &headers).await;
    }

    // Column projection — resolved ONCE here, before the format dispatch below, so
    // every non-arrow format (zip, hdf5, json, json-seq/html) restricts its walk
    // to the same top-level child set. Upstream applies `entry.read(fields=field)`
    // before `construct_data_response` dispatches on `format` (router.py:1440), so
    // the projection is format-agnostic. The POST entry point forwards its bare-
    // list body as repeated `field=` query keys (see `post_container_full`), so GET
    // and POST converge on this single resolution.
    let projection = repeated_query_values(&uri, &["field", "column"]);

    // Resolve effective media type once: format param beats Accept header. An
    // explicit but unserviceable format/Accept resolves to `None` → HTTP 406,
    // consistent with the array/table/sparse handlers (no silent HTML fallback).
    let family = crate::core::structures::StructureFamily::Container;
    let media_type = crate::serialization::negotiate_media_type(
        format_str.as_deref(),
        accept,
        family,
        &state.serialization_registry,
    )
    .ok_or_else(|| {
        unsupported_media_type(
            family,
            format_str.as_deref().unwrap_or(accept),
            &state.serialization_registry,
        )
    })?;
    let path = segments.join("/");

    // H3: compute access filter once (async) so it can be pushed into the
    // listing inside spawn_blocking. A full-container export reads child data,
    // so it needs read:data (parity with Python's curried_filter
    // scopes=["read:data"] for the deep export, router.py:1456).
    let access_filter = if let Some(ref policy) = state.access_policy {
        let requested = crate::auth::ScopeSet::from_iter([crate::auth::Scope::ReadData]);
        policy
            .list_filter(
                auth.principal.as_deref(),
                &auth.scopes,
                &requested,
                auth.authn_access_tags.as_deref(),
            )
            .await
    } else {
        None
    };

    // Deep-export branch (upstream tiled #660): two-phase build. Phase 1
    // walks the tree on the executor and collects owned leaf handles; phase 2
    // reads each leaf and deflates it into the zip. Container walk/search/get
    // are async (a blocking backend offloads internally); the per-leaf zip
    // deflate is the CPU-bound part and stays on `spawn_blocking`.
    if media_type == "application/zip" {
        // Phase 1: walk the container tree and collect a FLAT, ordered list of
        // leaf entries. Each leaf captures an OWNED Arc handle (via
        // as_array_arc/as_table_arc) — NOT decoded data — so the reads run in
        // phase 2. No read() happens in this phase.
        let walked_zip;
        let container: &dyn ContainerAdapter = if segments.is_empty() {
            state.root_tree.as_ref()
        } else {
            walked_zip = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
            walked_zip.as_container().ok_or_else(|| {
                ServerError::WrongType(format!("'{}' is not a container", segments.join("/")))
            })?
        };
        let max_depth: Option<usize> = params
            .get("max_depth")
            .and_then(|s| s.parse::<usize>().ok())
            .map(|d| d.min(DEPTH_LIMIT));
        let mut entries: Vec<ZipEntry> = Vec::new();
        // The zip export carries metadata in the JSON tree, not per-entry, so the
        // collected group metadata is discarded here.
        let mut group_metas: Vec<(String, serde_json::Value)> = Vec::new();
        collect_zip_entries(
            container,
            "",
            &path,
            access_filter.as_ref(),
            &mut entries,
            &mut group_metas,
            0,
            max_depth,
            projection.as_deref(),
        )
        .await?;

        // Phase 2: for each entry IN ORDER, read the leaf on the executor (no
        // `block_on`), then hand that single decoded leaf to a spawn_blocking
        // that deflates it into the ZipWriter and returns the writer. Read-one →
        // write-one keeps live memory bounded to one decoded leaf + the zip
        // state, exactly as the previous streaming loop did.
        //
        // Cumulative bytesize cap: decoded bytes across all leaves are summed and
        // checked against response_bytesize_limit after every Array/Table leaf.
        // Crumbs (small JSON metadata placeholders for unhandled families) are not
        // counted — they carry no decoded payload. The non-zip container listing
        // (Resource JSON metadata) has no decoded payload either and is uncapped.
        use zip::write::SimpleFileOptions;
        let opts =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        let mut writer: ZipBuf = zip::ZipWriter::new(Cursor::new(Vec::<u8>::new()));
        let mut cumulative_bytes: usize = 0;
        for ZipEntry { name, leaf, .. } in entries {
            match leaf {
                ZipLeaf::Array(arc) => {
                    let slice = crate::core::ndslice::NDSlice::empty();
                    let data = arc.read(&slice).await.map_err(ServerError::from)?;
                    cumulative_bytes += data.data.len();
                    check_response_size(
                        cumulative_bytes,
                        state.response_bytesize_limit,
                        "Select a subset of the data to request a smaller chunk.",
                    )?;
                    writer = tokio::task::spawn_blocking(move || -> Result<ZipBuf, ServerError> {
                        writer
                            .start_file(name, opts)
                            .map_err(|e| ServerError::Internal(format!("zip: {e}")))?;
                        writer
                            .write_all(&data.data)
                            .map_err(|e| ServerError::Internal(format!("zip write: {e}")))?;
                        Ok(writer)
                    })
                    .await
                    .map_err(|e| ServerError::Internal(format!("zip write task failed: {e}")))??;
                }
                ZipLeaf::Table(arc) => {
                    let table = arc.read(None).await.map_err(ServerError::from)?;
                    let leaf_bytes: usize = table
                        .batches
                        .iter()
                        .map(|b| b.get_array_memory_size())
                        .sum();
                    cumulative_bytes += leaf_bytes;
                    check_response_size(
                        cumulative_bytes,
                        state.response_bytesize_limit,
                        "Select a subset of the data to request a smaller chunk.",
                    )?;
                    writer = tokio::task::spawn_blocking(move || -> Result<ZipBuf, ServerError> {
                        let mut ipc_bytes = Vec::new();
                        {
                            let mut writer_ipc = arrow::ipc::writer::FileWriter::try_new(
                                &mut ipc_bytes,
                                &table.schema,
                            )
                            .map_err(|e| ServerError::Internal(format!("arrow ipc: {e}")))?;
                            for batch in &table.batches {
                                writer_ipc.write(batch).map_err(|e| {
                                    ServerError::Internal(format!("arrow ipc: {e}"))
                                })?;
                            }
                            writer_ipc
                                .finish()
                                .map_err(|e| ServerError::Internal(format!("arrow ipc: {e}")))?;
                        }
                        writer
                            .start_file(name, opts)
                            .map_err(|e| ServerError::Internal(format!("zip: {e}")))?;
                        writer
                            .write_all(&ipc_bytes)
                            .map_err(|e| ServerError::Internal(format!("zip write: {e}")))?;
                        Ok(writer)
                    })
                    .await
                    .map_err(|e| ServerError::Internal(format!("zip write task failed: {e}")))??;
                }
                ZipLeaf::Crumb(crumb_bytes) => {
                    writer = tokio::task::spawn_blocking(move || -> Result<ZipBuf, ServerError> {
                        writer
                            .start_file(name, opts)
                            .map_err(|e| ServerError::Internal(format!("zip: {e}")))?;
                        writer
                            .write_all(&crumb_bytes)
                            .map_err(|e| ServerError::Internal(format!("zip write: {e}")))?;
                        Ok(writer)
                    })
                    .await
                    .map_err(|e| ServerError::Internal(format!("zip write task failed: {e}")))??;
                }
            }
        }
        let buf = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, ServerError> {
            let cursor = writer
                .finish()
                .map_err(|e| ServerError::Internal(format!("zip finalize: {e}")))?;
            Ok(cursor.into_inner())
        })
        .await
        .map_err(|e| ServerError::Internal(format!("zip finalize task failed: {e}")))??;
        return Ok(serve_with_range(
            &headers,
            "application/zip",
            bytes::Bytes::from(buf),
            filename_str.as_deref(),
        ));
    }

    // Deep-export to a single HDF5 file — Python container.serialize_hdf5
    // (serialization/container.py:46). Like the zip branch this needs the
    // adapter tree (not just bytes), so the walk + per-leaf read happen here:
    // each numeric array → a dataset, each table column → its own 1-D dataset,
    // intermediate containers → groups.
    #[cfg(feature = "hdf5-serializer")]
    if media_type == crate::core::media_type::mime::HDF5 {
        let h5 = container_full_hdf5(
            &state,
            &segments,
            &path,
            access_filter.as_ref(),
            &params,
            projection.as_deref(),
        )
        .await?;
        return Ok(serve_with_range(
            &headers,
            crate::core::media_type::mime::HDF5,
            h5,
            filename_str.as_deref(),
        ));
    }

    // Non-zip: resolve the container on the executor (async walk/keys/search/get).
    let walked_nonzip;
    let container: &dyn ContainerAdapter = if segments.is_empty() {
        state.root_tree.as_ref()
    } else {
        walked_nonzip = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
        walked_nonzip.as_container().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not a container", segments.join("/")))
        })?
    };

    // Assemble the body for the registered Container serializer. application/json
    // is the recursive `{contents, metadata}` tree (Python `serialize_json`,
    // container.py:91-115); text/html and application/json-seq consume the
    // immediate-children Resource listing.
    let body_json = if media_type == crate::core::media_type::mime::JSON {
        // Root node: the container's own metadata plus the recursively-built
        // child tree. The access filter is applied at every level inside the
        // helper (parity with Python's per-node `filter_for_access`).
        let contents =
            build_container_json_contents(container, access_filter.as_ref(), projection.as_deref())
                .await?;
        let mut tree = serde_json::Map::new();
        tree.insert("contents".into(), serde_json::Value::Object(contents));
        tree.insert("metadata".into(), container.metadata().clone());
        serde_json::to_vec(&serde_json::Value::Object(tree))
            .map_err(|e| ServerError::Internal(format!("encode: {e}")))?
    } else {
        // H3: apply access filter to listing.
        let queries: Vec<crate::core::queries::Query> = access_filter
            .map(|f| vec![crate::core::queries::Query::AccessBlobFilter(f)])
            .unwrap_or_default();
        let has_access_filter = !queries.is_empty();
        let visible_keys = if queries.is_empty() {
            container.keys().await?
        } else {
            // An unsupported query variant propagates as HTTP 400.
            container.search(&queries).await?
        };
        // Column projection: restrict the listing to the requested fields, in
        // field order. Validation is against the FULL child set (upstream
        // `read(fields)` checks the whole mapping before access filtering); the
        // access-visible set only decides which projected fields survive.
        let visible_keys = match projection.as_deref() {
            None => visible_keys,
            Some(fields) => {
                let all_keys = if has_access_filter {
                    container.keys().await?
                } else {
                    // no access filter → visible_keys is already the full set
                    visible_keys.clone()
                };
                apply_child_projection(&all_keys, &visible_keys, fields)?
            }
        };
        let mut children: Vec<crate::core::schemas::Resource> = Vec::new();
        for k in &visible_keys {
            let Some(child) = container.get(k).await? else {
                continue;
            };
            let child_path = if path.is_empty() {
                k.clone()
            } else {
                format!("{path}/{k}")
            };
            children.push(core::construct_resource(&child, k, &child_path, &base_url).await?);
        }
        serde_json::to_vec(&children).map_err(|e| ServerError::Internal(format!("encode: {e}")))?
    };

    let body = if let Some(serializer) = state.serialization_registry.dispatch(
        crate::core::structures::StructureFamily::Container,
        &media_type,
    ) {
        let meta = serde_json::json!({"path": path});
        // Offload the (CPU-bound) container serializer off the async executor.
        tokio::task::spawn_blocking(move || serializer(&body_json, &meta))
            .await
            .map_err(|e| ServerError::Internal(format!("serialize task failed: {e}")))?
            .map_err(map_serialize_error)?
    } else {
        return Err(ServerError::Validation(format!(
            "no container serializer for {media_type}"
        )));
    };

    Ok(serve_with_range(
        &headers,
        &media_type,
        body,
        filename_str.as_deref(),
    ))
}

/// One leaf to bundle into a deep-export zip. Phase 1 captures only an OWNED
/// Arc handle (no decoded data) so the read can run on the executor in phase 2.
enum ZipLeaf {
    Array(Arc<dyn crate::core::adapters::ArrayAdapterRead>),
    Table(Arc<dyn crate::core::adapters::TableAdapterRead>),
    /// Pre-serialized `.json` crumb for families not yet bundled
    /// (Sparse/Awkward). No read is needed, so the bytes are captured directly.
    Crumb(Vec<u8>),
}

/// One ordered entry in the deep-export zip.
struct ZipEntry {
    /// Full in-zip filename including extension (`.bin` / `.arrow` / `.json`).
    name: String,
    leaf: ZipLeaf,
    /// The leaf node's own metadata. Read only by the HDF5 export
    /// (`container_full_hdf5`), where it becomes dataset/group attributes; the zip
    /// export carries metadata in the JSON tree instead and ignores it. Hence it
    /// is genuinely unread when the `hdf5-serializer` feature is off — a
    /// feature-conditional field, not dead code. `Null` for crumbs.
    #[cfg_attr(not(feature = "hdf5-serializer"), allow(dead_code))]
    metadata: serde_json::Value,
}

/// The owned-buffer ZipWriter that ping-pongs through `spawn_blocking` in
/// phase 2 (one write per leaf, returned for the next iteration).
type ZipBuf = zip::ZipWriter<Cursor<Vec<u8>>>;

/// One leaf read into an owned, `Send` description for the HDF5 deep-export.
/// Phase A (executor) reads each leaf into this; phase B (`spawn_blocking`)
/// drives the `!Send` HDF5 builder from it.
#[cfg(feature = "hdf5-serializer")]
enum CollectedH5Leaf {
    Array {
        /// Parent group path ("a/b"); empty = file root.
        group_path: String,
        /// Dataset name (the array's own key).
        name: String,
        data: bytes::Bytes,
        /// numpy dtype kind char (`f`/`i`/`u`).
        kind: char,
        itemsize: usize,
        big_endian: bool,
        shape: Vec<usize>,
        /// The array node's metadata → dataset attributes.
        metadata: serde_json::Value,
    },
    Table {
        /// Group path for the table (its key); each column becomes a dataset
        /// beneath it.
        group_path: String,
        batch: arrow::record_batch::RecordBatch,
        /// The table node's metadata → group attributes.
        metadata: serde_json::Value,
    },
}

/// Deep-export the container subtree as a single HDF5 file — Python
/// `container.serialize_hdf5`. Walks like the zip export, reads each leaf on the
/// executor (phase A), then builds the HDF5 tree on `spawn_blocking` (phase B,
/// since rust-hdf5 is blocking and `!Send`). Numeric arrays and table columns
/// become 1-D datasets; intermediate containers become groups. Sparse/awkward
/// and depth-truncated leaves (the zip export's JSON "crumbs") are skipped —
/// HDF5 has no crumb placeholder, and Python's `walk` yields no dataset shape
/// for those families either.
#[cfg(feature = "hdf5-serializer")]
async fn container_full_hdf5(
    state: &AppState,
    segments: &[String],
    path: &str,
    access_filter: Option<&crate::core::queries::AccessBlobFilter>,
    params: &HashMap<String, String>,
    projection: Option<&[String]>,
) -> Result<bytes::Bytes, ServerError> {
    use crate::serialization::hdf5_container::Hdf5TreeBuilder;

    let walked;
    let container: &dyn ContainerAdapter = if segments.is_empty() {
        state.root_tree.as_ref()
    } else {
        walked = core::walk_tree(state.root_tree.as_ref(), segments).await?;
        walked.as_container().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not a container", segments.join("/")))
        })?
    };
    let max_depth: Option<usize> = params
        .get("max_depth")
        .and_then(|s| s.parse::<usize>().ok())
        .map(|d| d.min(DEPTH_LIMIT));

    // The export root's metadata becomes the HDF5 file (root group) attributes —
    // Python `file.attrs.update(metadata)`.
    let root_metadata = container.metadata().clone();

    let mut entries: Vec<ZipEntry> = Vec::new();
    // (group_path, metadata) for each intermediate container → group attributes.
    let mut group_metas: Vec<(String, serde_json::Value)> = Vec::new();
    collect_zip_entries(
        container,
        "",
        path,
        access_filter,
        &mut entries,
        &mut group_metas,
        0,
        max_depth,
        projection,
    )
    .await?;

    // Phase A: read each leaf on the executor into an owned, Send description.
    // The same cumulative bytesize cap as the zip export guards memory (the whole
    // tree is held decoded before the single-file build).
    let mut collected: Vec<CollectedH5Leaf> = Vec::new();
    let mut cumulative_bytes: usize = 0;
    for ZipEntry {
        name,
        leaf,
        metadata,
    } in entries
    {
        match leaf {
            ZipLeaf::Array(arc) => {
                let slice = crate::core::ndslice::NDSlice::empty();
                let nd = arc.read(&slice).await.map_err(ServerError::from)?;
                cumulative_bytes += nd.data.len();
                check_response_size(
                    cumulative_bytes,
                    state.response_bytesize_limit,
                    "Select a subset of the data to request a smaller chunk.",
                )?;
                // "a/b/key.bin" → group "a/b", dataset "key"; "key.bin" → root.
                let stem = name.strip_suffix(".bin").unwrap_or(&name);
                let (group_path, leaf_name) = match stem.rsplit_once('/') {
                    Some((g, l)) => (g.to_string(), l.to_string()),
                    None => (String::new(), stem.to_string()),
                };
                collected.push(CollectedH5Leaf::Array {
                    group_path,
                    name: leaf_name,
                    kind: nd.dtype.kind.to_numpy_char(),
                    itemsize: nd.dtype.element_size(),
                    big_endian: nd.dtype.endianness.to_numpy_char() == '>',
                    shape: nd.shape.clone(),
                    data: nd.data,
                    metadata,
                });
            }
            ZipLeaf::Table(arc) => {
                let table = arc.read(None).await.map_err(ServerError::from)?;
                let leaf_bytes: usize = table
                    .batches
                    .iter()
                    .map(|b| b.get_array_memory_size())
                    .sum();
                cumulative_bytes += leaf_bytes;
                check_response_size(
                    cumulative_bytes,
                    state.response_bytesize_limit,
                    "Select a subset of the data to request a smaller chunk.",
                )?;
                // Concat multi-batch streams so each column is one contiguous
                // dataset. "a/b/t.arrow" → group "a/b/t" with one dataset per
                // column (the table key becomes a group, per Python's walk).
                let batch = if table.batches.is_empty() {
                    arrow::record_batch::RecordBatch::new_empty(table.schema.clone())
                } else if table.batches.len() == 1 {
                    table.batches.into_iter().next().unwrap()
                } else {
                    arrow::compute::concat_batches(&table.schema, &table.batches)
                        .map_err(|e| ServerError::Internal(format!("arrow concat: {e}")))?
                };
                let group_path = name.strip_suffix(".arrow").unwrap_or(&name).to_string();
                collected.push(CollectedH5Leaf::Table {
                    group_path,
                    batch,
                    metadata,
                });
            }
            ZipLeaf::Crumb(_) => continue,
        }
    }

    // Phase B: build the HDF5 tree off the async executor.
    let h5 = tokio::task::spawn_blocking(move || -> Result<bytes::Bytes, ServerError> {
        let mut builder =
            Hdf5TreeBuilder::new().map_err(|e| ServerError::Internal(format!("hdf5 init: {e}")))?;
        // Root attrs, then register every group's attrs BEFORE the leaves so the
        // attributes land the moment each group is created by `ensure_group`.
        builder
            .set_root_attrs(&root_metadata)
            .map_err(|e| ServerError::Internal(format!("hdf5 root attrs: {e}")))?;
        for (group_path, meta) in group_metas {
            builder.register_group_attrs(group_path, meta);
        }
        for leaf in collected {
            match leaf {
                CollectedH5Leaf::Array {
                    group_path,
                    name,
                    data,
                    kind,
                    itemsize,
                    big_endian,
                    shape,
                    metadata,
                } => {
                    builder
                        .add_array(
                            &group_path,
                            &name,
                            &data,
                            kind,
                            itemsize,
                            big_endian,
                            &shape,
                            &metadata,
                        )
                        .map_err(|e| ServerError::Internal(format!("hdf5 array '{name}': {e}")))?;
                }
                CollectedH5Leaf::Table {
                    group_path,
                    batch,
                    metadata,
                } => {
                    builder
                        .add_table_columns(&group_path, &batch, &metadata)
                        .map_err(|e| {
                            ServerError::Internal(format!("hdf5 table '{group_path}': {e}"))
                        })?;
                }
            }
        }
        builder
            .finish()
            .map_err(|e| ServerError::Internal(format!("hdf5 finish: {e}")))
    })
    .await
    .map_err(|e| ServerError::Internal(format!("hdf5 build task failed: {e}")))??;

    Ok(h5)
}

/// Maximum walk depth for zip export — mirrors Python `DEPTH_LIMIT = 5`
/// (`tiled/server/core.py:62`).
const DEPTH_LIMIT: usize = 5;

/// Phase 1 of the deep-export: recursively collect every visible leaf below
/// `container` into a flat, ordered `out` list. Runs on the executor — the
/// async container `search`/`keys`/`get` resolve through the adapter (a
/// blocking backend offloads internally). It captures OWNED Arc handles via
/// `as_array_arc`/`as_table_arc` and performs NO `read()`; the leaf reads run
/// in phase 2. Returns a [`BoxFuture`] so the recursive call type-checks.
/// H3: `access_filter` (when Some) is applied at each level to skip children
/// the caller is not permitted to see.
/// `current_depth` is the depth of `container` relative to the export root
/// (0 = root). `max_depth` caps the walk; `None` means unlimited.
#[allow(clippy::too_many_arguments)]
fn collect_zip_entries<'a>(
    container: &'a dyn ContainerAdapter,
    prefix: &'a str,
    base_path: &'a str,
    access_filter: Option<&'a crate::core::queries::AccessBlobFilter>,
    out: &'a mut Vec<ZipEntry>,
    // Intermediate-container metadata collected as `(group_path, metadata)` for
    // the HDF5 export's group attributes. The zip export passes a throwaway Vec
    // and ignores it.
    group_metas: &'a mut Vec<(String, serde_json::Value)>,
    current_depth: usize,
    max_depth: Option<usize>,
    // Column projection, applied only at the export root (`current_depth == 0`);
    // nested containers are serialized whole, matching upstream `read(fields)`
    // which projects only the top-level mapping (mapping.py:280-294).
    projection: Option<&'a [String]>,
) -> crate::core::adapters::BoxFuture<'a, Result<(), ServerError>> {
    Box::pin(async move {
        let visible_keys = match access_filter {
            // AccessBlobFilter is supported by the catalog/map adapters used
            // here; an adapter that cannot evaluate it propagates HTTP 400.
            Some(f) => {
                container
                    .search(&[crate::core::queries::Query::AccessBlobFilter(f.clone())])
                    .await?
            }
            None => container.keys().await?,
        };
        // Restrict the root level to the requested fields, in field order.
        // Validation is against the FULL child set (upstream `read(fields)`
        // checks the whole mapping before access filtering).
        let visible_keys = match projection {
            Some(fields) if current_depth == 0 => {
                let all_keys = if access_filter.is_some() {
                    container.keys().await?
                } else {
                    // no access filter → visible_keys is already the full set
                    visible_keys.clone()
                };
                apply_child_projection(&all_keys, &visible_keys, fields)?
            }
            _ => visible_keys,
        };
        for key in visible_keys {
            let Some(child) = container.get(&key).await? else {
                continue;
            };
            let entry_name = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}/{key}")
            };
            // The child's own metadata, captured before `as_*` so it is available
            // for the HDF5 export regardless of which family branch is taken.
            let child_meta = child.metadata().clone();
            if let Some(arc) = child.as_array_arc() {
                out.push(ZipEntry {
                    name: format!("{entry_name}.bin"),
                    leaf: ZipLeaf::Array(arc),
                    metadata: child_meta,
                });
            } else if let Some(arc) = child.as_table_arc() {
                out.push(ZipEntry {
                    name: format!("{entry_name}.arrow"),
                    leaf: ZipLeaf::Table(arc),
                    metadata: child_meta,
                });
            } else if let Some(child_c) = child.as_container() {
                if max_depth.is_some_and(|d| current_depth >= d) {
                    // Depth cap reached: emit a crumb so the client knows the
                    // subtree was truncated rather than silently missing.
                    let crumb = serde_json::json!({
                        "path": format!("{base_path}/{entry_name}"),
                        "structure_family": "container",
                        "note": "subtree not exported: max_depth reached",
                    });
                    let crumb_bytes = serde_json::to_vec(&crumb)
                        .map_err(|e| ServerError::Internal(format!("json: {e}")))?;
                    out.push(ZipEntry {
                        name: format!("{entry_name}.json"),
                        leaf: ZipLeaf::Crumb(crumb_bytes),
                        metadata: serde_json::Value::Null,
                    });
                } else {
                    // Record this container's metadata for its HDF5 group attrs,
                    // then recurse into it.
                    group_metas.push((entry_name.clone(), child_meta));
                    collect_zip_entries(
                        child_c,
                        &entry_name,
                        base_path,
                        access_filter,
                        out,
                        group_metas,
                        current_depth + 1,
                        max_depth,
                        // Projection is root-only; the `current_depth == 0` guard
                        // above ignores it below the root, so forwarding it here is
                        // inert but keeps the recursion signature uniform.
                        projection,
                    )
                    .await?;
                }
            } else {
                // Sparse/Awkward and any future family: drop a crumb describing
                // the leaf (identical to the previous behaviour).
                let crumb = serde_json::json!({
                    "path": format!("{base_path}/{entry_name}"),
                    "structure_family": format!("{:?}", child.structure_family()),
                    "note": "leaf format not yet bundled in deep export",
                });
                let crumb_bytes = serde_json::to_vec(&crumb)
                    .map_err(|e| ServerError::Internal(format!("json: {e}")))?;
                out.push(ZipEntry {
                    name: format!("{entry_name}.json"),
                    leaf: ZipLeaf::Crumb(crumb_bytes),
                    metadata: serde_json::Value::Null,
                });
            }
        }
        Ok(())
    })
}

/// Build the `contents` map of the Python container `application/json` tree
/// (`serialize_json` + `walk`, `tiled/serialization/container.py:14-115`) for
/// `container`: one `{"contents", "metadata"}` node per visible child.
///
/// - array → leaf node (`contents: {}` plus the child's own metadata).
/// - table → its columns become synthetic array children, each with empty
///   `contents` and empty `metadata`. Python yields one `walk` entry per column
///   and navigates to `table[col]`, an `ArrayAdapter.from_array(...)` whose
///   `metadata or {}` is `{}` (container.py:33-35, adapters/arrow.py:184,
///   adapters/core.py:24).
/// - container → recurse; an EMPTY container (no array/table descendant at any
///   depth) is OMITTED, because Python's `walk` only yields at array leaves and
///   table columns, so a subtree with none is never added to the tree.
/// - sparse/awkward → leaf node. Python's `walk` has no branch for these (they
///   fall into `else: filtered.items()` and raise `AttributeError`), so Rust
///   emits a leaf to degrade gracefully rather than return 500.
///
/// `access_filter` (when `Some`) is applied at each container level, matching
/// the deep-export walk and Python's per-node `filter_for_access`.
fn build_container_json_contents<'a>(
    container: &'a dyn ContainerAdapter,
    access_filter: Option<&'a crate::core::queries::AccessBlobFilter>,
    // Column projection, applied only at the root node (this function recurses
    // with `None`); nested children are serialized whole, matching upstream
    // `read(fields)` which projects only the top-level mapping (mapping.py:280).
    projection: Option<&'a [String]>,
) -> crate::core::adapters::BoxFuture<
    'a,
    Result<serde_json::Map<String, serde_json::Value>, ServerError>,
> {
    Box::pin(async move {
        let visible_keys = match access_filter {
            Some(f) => {
                container
                    .search(&[crate::core::queries::Query::AccessBlobFilter(f.clone())])
                    .await?
            }
            None => container.keys().await?,
        };
        // Restrict the root level to the requested fields, in field order.
        // Validation is against the FULL child set (upstream `read(fields)`
        // checks the whole mapping before access filtering).
        let visible_keys = match projection {
            None => visible_keys,
            Some(fields) => {
                let all_keys = if access_filter.is_some() {
                    container.keys().await?
                } else {
                    // no access filter → visible_keys is already the full set
                    visible_keys.clone()
                };
                apply_child_projection(&all_keys, &visible_keys, fields)?
            }
        };
        let mut contents = serde_json::Map::new();
        for key in visible_keys {
            let Some(child) = container.get(&key).await? else {
                continue;
            };
            let node = match child.structure_family() {
                crate::core::structures::StructureFamily::Container => {
                    let sub = build_container_json_contents(
                        child
                            .as_container()
                            .expect("container family => as_container"),
                        access_filter,
                        // Projection is root-only; nested containers serialize whole.
                        None,
                    )
                    .await?;
                    if sub.is_empty() {
                        continue;
                    }
                    let mut node = serde_json::Map::new();
                    node.insert("contents".into(), serde_json::Value::Object(sub));
                    node.insert("metadata".into(), child.metadata().clone());
                    serde_json::Value::Object(node)
                }
                crate::core::structures::StructureFamily::Table => {
                    let columns = child
                        .as_table()
                        .expect("table family => as_table")
                        .structure()
                        .columns
                        .clone();
                    if columns.is_empty() {
                        continue;
                    }
                    let mut col_contents = serde_json::Map::new();
                    for col in columns {
                        col_contents
                            .insert(col, serde_json::json!({"contents": {}, "metadata": {}}));
                    }
                    let mut node = serde_json::Map::new();
                    node.insert("contents".into(), serde_json::Value::Object(col_contents));
                    node.insert("metadata".into(), child.metadata().clone());
                    serde_json::Value::Object(node)
                }
                _ => {
                    // array / sparse / awkward => leaf.
                    let mut node = serde_json::Map::new();
                    node.insert(
                        "contents".into(),
                        serde_json::Value::Object(serde_json::Map::new()),
                    );
                    node.insert("metadata".into(), child.metadata().clone());
                    serde_json::Value::Object(node)
                }
            };
            contents.insert(key, node);
        }
        Ok(contents)
    })
}

// ---------------------------------------------------------------------------
// `?block=` parser + multi-chunk read (upstream tiled PR #1302)
// ---------------------------------------------------------------------------

/// One axis of a block selection. `Single` is the historical
/// integer-index form; `Range` (start..stop) spans a chunk range.
#[derive(Debug, Clone, Copy)]
enum BlockSpec {
    Single(usize),
    Range(usize, usize),
}

impl BlockSpec {
    fn parse(piece: &str) -> Result<Self, ServerError> {
        if let Some((s, t)) = piece.split_once(':') {
            let start: usize = s
                .parse()
                .map_err(|_| ServerError::Validation(format!("Invalid block range '{piece}'")))?;
            let stop: usize = t
                .parse()
                .map_err(|_| ServerError::Validation(format!("Invalid block range '{piece}'")))?;
            if stop <= start {
                return Err(ServerError::Validation(format!(
                    "Block range '{piece}': stop ({stop}) must be > start ({start})"
                )));
            }
            Ok(BlockSpec::Range(start, stop))
        } else {
            piece
                .parse::<usize>()
                .map(BlockSpec::Single)
                .map_err(|_| ServerError::Validation(format!("Invalid block index '{piece}'")))
        }
    }

    fn range(self) -> (usize, usize) {
        match self {
            BlockSpec::Single(i) => (i, i + 1),
            BlockSpec::Range(s, t) => (s, t),
        }
    }
}

/// Walk the cartesian product of chunks the request spans, read each
/// one with an empty slice, and concat them in row-major order into a
/// single result buffer.
async fn read_block_range(
    adapter: &dyn crate::core::adapters::ArrayAdapterRead,
    block_specs: &[BlockSpec],
) -> Result<crate::core::dtype::DynNDArray, ServerError> {
    let structure = adapter.structure();
    let chunks = &structure.chunks;
    if block_specs.len() != chunks.len() {
        return Err(ServerError::Validation(format!(
            "Block parameter must have {} comma-separated parameters (got {})",
            chunks.len(),
            block_specs.len()
        )));
    }
    // Per-axis chunk range + bounds check.
    let mut axis_ranges: Vec<(usize, usize)> = Vec::with_capacity(block_specs.len());
    for (axis, spec) in block_specs.iter().enumerate() {
        let (start, stop) = spec.range();
        if stop > chunks[axis].len() {
            return Err(ServerError::BadRequest(format!(
                "Block range axis {axis}: stop {stop} exceeds chunk count {}",
                chunks[axis].len()
            )));
        }
        axis_ranges.push((start, stop));
    }
    // Result shape per axis = sum of chunk sizes spanned on that axis.
    let result_shape: Vec<usize> = axis_ranges
        .iter()
        .zip(chunks.iter())
        .map(|((start, stop), axis_chunks)| axis_chunks[*start..*stop].iter().sum())
        .collect();

    // Probe element size by reading the first chunk (cheap — same chunk
    // we'd read anyway). We'll reuse it as the result block 0.
    let first_idx: Vec<usize> = axis_ranges.iter().map(|(s, _)| *s).collect();
    let first = adapter
        .read_block(&first_idx, &crate::core::ndslice::NDSlice::empty())
        .await
        .map_err(ServerError::from)?;
    let elem_size = first.dtype.element_size();
    let total: usize = result_shape.iter().product();
    let mut buf = vec![0u8; total * elem_size];

    // Pre-compute byte offsets in result for axis index `i_axis`:
    // result_axis_offsets[axis][i] = sum of chunk sizes [start..i] in elements.
    let result_axis_offsets: Vec<Vec<usize>> = axis_ranges
        .iter()
        .zip(chunks.iter())
        .map(|((start, stop), axis_chunks)| {
            let mut acc = 0usize;
            (*start..*stop)
                .map(|c| {
                    let here = acc;
                    acc += axis_chunks[c];
                    here
                })
                .collect::<Vec<_>>()
        })
        .collect();

    // Iterate chunks in row-major order and copy into result.
    let chunk_count: Vec<usize> = axis_ranges.iter().map(|(s, t)| t - s).collect();
    let total_chunks: usize = chunk_count.iter().product();
    let mut idx_in_range = vec![0usize; chunk_count.len()];
    let mut copied_first = false;

    for _step in 0..total_chunks {
        let chunk_global_idx: Vec<usize> = idx_in_range
            .iter()
            .zip(axis_ranges.iter())
            .map(|(i, (s, _))| s + i)
            .collect();
        let chunk_data = if !copied_first && chunk_global_idx == first_idx {
            copied_first = true;
            first.clone()
        } else {
            adapter
                .read_block(&chunk_global_idx, &crate::core::ndslice::NDSlice::empty())
                .await
                .map_err(ServerError::from)?
        };
        let chunk_offsets: Vec<usize> = idx_in_range
            .iter()
            .zip(result_axis_offsets.iter())
            .map(|(i, off)| off[*i])
            .collect();
        copy_chunk_into_result(
            &mut buf,
            &result_shape,
            &chunk_offsets,
            &chunk_data.data,
            &chunk_data.shape,
            elem_size,
        );
        // Advance the per-axis index in row-major order.
        for axis in (0..idx_in_range.len()).rev() {
            idx_in_range[axis] += 1;
            if idx_in_range[axis] < chunk_count[axis] {
                break;
            }
            idx_in_range[axis] = 0;
        }
    }

    Ok(crate::core::dtype::DynNDArray::new(
        bytes::Bytes::from(buf),
        first.dtype,
        result_shape,
    ))
}

/// Copy a single chunk's row-major bytes into the right offset of the
/// (also row-major) result buffer. Walks each "row" — the innermost
/// axis is contiguous — and computes the destination offset per row
/// from the per-axis chunk offsets and the result's strides.
fn copy_chunk_into_result(
    result: &mut [u8],
    result_shape: &[usize],
    chunk_offsets: &[usize],
    chunk: &[u8],
    chunk_shape: &[usize],
    elem_size: usize,
) {
    let ndim = result_shape.len();
    if ndim == 0 || chunk_shape.contains(&0) {
        return;
    }
    if ndim == 1 {
        let dst = chunk_offsets[0] * elem_size;
        let len = chunk_shape[0] * elem_size;
        result[dst..dst + len].copy_from_slice(&chunk[..len]);
        return;
    }
    // Strides in bytes (row-major, innermost stride = elem_size).
    let mut result_strides = vec![elem_size; ndim];
    for i in (0..ndim - 1).rev() {
        result_strides[i] = result_strides[i + 1] * result_shape[i + 1];
    }
    let mut chunk_strides = vec![elem_size; ndim];
    for i in (0..ndim - 1).rev() {
        chunk_strides[i] = chunk_strides[i + 1] * chunk_shape[i + 1];
    }
    let inner = ndim - 1;
    let row_bytes = chunk_shape[inner] * elem_size;
    let outer_total: usize = chunk_shape[..inner].iter().product();

    let mut outer = vec![0usize; inner];
    for _row in 0..outer_total {
        let mut src = 0usize;
        let mut dst = chunk_offsets[inner] * elem_size;
        for axis in 0..inner {
            src += outer[axis] * chunk_strides[axis];
            dst += (chunk_offsets[axis] + outer[axis]) * result_strides[axis];
        }
        result[dst..dst + row_bytes].copy_from_slice(&chunk[src..src + row_bytes]);
        // Increment outer in row-major order.
        for axis in (0..inner).rev() {
            outer[axis] += 1;
            if outer[axis] < chunk_shape[axis] {
                break;
            }
            outer[axis] = 0;
        }
    }
}

// ---------------------------------------------------------------------------
// GET /api/v1/array/full/{*path}
// ---------------------------------------------------------------------------
//
// Returns the entire array with optional `?slice=...` numpy-style slicing.
// Calls ArrayAdapterRead::read, which assembles all chunks. Mirrors upstream
// tiled's `/array/full/` endpoint, which the SPA uses via `links.full`.
pub async fn array_full(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/array/full/");
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::ReadData,
    )
    .await?;

    let slice_str = params
        .get("slice")
        .map(|s| s.to_string())
        .unwrap_or_default();
    let format_str = params.get("format").map(|s| s.to_string());
    let filename_str = params.get("filename").map(|s| s.to_string());
    // The async tree walk resolves each hop on the executor and hands back an
    // owned `Arc` clone of the leaf; the read future offloads its own blocking
    // and is awaited on the executor below.
    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;

    let slice = match slice_str.as_str() {
        "" => crate::core::ndslice::NDSlice::empty(),
        s => crate::core::ndslice::NDSlice::from_numpy_str(s)
            .map_err(|e| ServerError::Validation(format!("Invalid slice '{s}': {e}")))?,
    };

    // Sparse leaf: serve the full COO table (Python serialization/sparse.py),
    // applying the optional `?slice=` via the adapter's `read`.
    if let Some(sparse) = adapter.as_sparse_arc() {
        let data = sparse.read(&slice).await.map_err(ServerError::from)?;
        return build_sparse_response(
            data,
            format_str.as_deref(),
            &headers,
            &state,
            filename_str.as_deref(),
        )
        .await;
    }

    let array_adapter: Arc<dyn crate::core::adapters::ArrayAdapterRead> =
        adapter.as_array_arc().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not an array", segments.join("/")))
        })?;

    let data = array_adapter
        .read(&slice)
        .await
        .map_err(ServerError::from)?;

    build_array_response(
        data,
        format_str.as_deref(),
        &headers,
        &state,
        filename_str.as_deref(),
    )
    .await
}

// ---------------------------------------------------------------------------
// GET /api/v1/ragged/full/{*path}
// ---------------------------------------------------------------------------

/// Serve a ragged (variable-length row) array. Mirrors Python `get_ragged_full`
/// (router.py:838-906): resolve the entry, read the (optionally sliced) array,
/// then negotiate and serialize. A slice that Awkward cannot apply surfaces as
/// HTTP 422 (Python `RaggedSlicingError` → 422, router.py:873-880), not 500.
///
/// Python advertises a `block` link for ragged too (links.py:43), but only
/// `GET /ragged/full` exists as a read route — the `block` link targets the
/// PUT-only `/ragged/block` write endpoint — so no GET block handler is added.
pub async fn ragged_full(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/ragged/full/");
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::ReadData,
    )
    .await?;

    let slice_str = params
        .get("slice")
        .map(|s| s.to_string())
        .unwrap_or_default();
    let format_str = params.get("format").map(|s| s.to_string());
    let filename_str = params.get("filename").map(|s| s.to_string());
    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;

    let slice = match slice_str.as_str() {
        "" => crate::core::ndslice::NDSlice::empty(),
        s => crate::core::ndslice::NDSlice::from_numpy_str(s)
            .map_err(|e| ServerError::Validation(format!("Invalid slice '{s}': {e}")))?,
    };

    let ragged = adapter.as_ragged_arc().ok_or_else(|| {
        ServerError::WrongType(format!("'{}' is not a ragged array", segments.join("/")))
    })?;

    // A slice Awkward cannot apply (out-of-bounds row index, scalar reduction,
    // index past a short ragged row) is a client error → 422, matching Python's
    // `RaggedSlicingError` handling (router.py:873-880), not a 500.
    let data = ragged.read(&slice).await.map_err(|e| match e {
        crate::core::TiledError::InvalidSlice(msg) => ServerError::Validation(format!(
            "Cannot apply the requested slice to the given ragged array: {msg}. \
             Try reading the entire array and slicing it on the client side instead."
        )),
        other => ServerError::from(other),
    })?;

    build_ragged_response(
        data,
        format_str.as_deref(),
        &headers,
        &state,
        filename_str.as_deref(),
    )
    .await
}

// ---------------------------------------------------------------------------
// Ragged write handlers: PUT /ragged/full, PUT /ragged/block, PATCH /ragged/full
// ---------------------------------------------------------------------------
//
// Mirror Python `put_ragged_full` / `put_ragged_block` / `patch_ragged_full`
// (router.py:908-1047). Only an internally-managed ragged node whose backing
// SQLite database lives under the server's writable storage is writable — the
// resolver decides this and gates `as_writable()` on it — so a non-writable
// node answers 405. Without the `sql-adapter` feature no ragged node is ever
// writable, so these routes uniformly answer 405; the routes themselves are
// unconditional because they reference only the always-present ragged write
// trait and the ZIP decoder, never the SQL backend directly.

/// Parse a `?`-query boolean the way FastAPI does: absent → `default`; a value
/// of `false`/`0`/`no`/`off` (case-insensitive) is false, anything else true.
fn query_bool(params: &HashMap<String, String>, key: &str, default: bool) -> bool {
    match params.get(key) {
        None => default,
        Some(v) => !matches!(
            v.to_ascii_lowercase().as_str(),
            "false" | "0" | "no" | "off"
        ),
    }
}

/// Parse a comma-separated list of non-negative integers (`?offset=`, `?shape=`,
/// `?block=`). A malformed value is a 400, matching Python's query-param
/// patterns (`^[0-9]+(,[0-9]+)*$`).
fn parse_csv_usize(s: &str) -> Result<Vec<usize>, ServerError> {
    s.split(',')
        .map(|p| {
            p.trim().parse::<usize>().map_err(|_| {
                ServerError::BadRequest(format!(
                    "expected comma-separated non-negative integers, got {s:?}"
                ))
            })
        })
        .collect()
}

/// The request `Content-Type`, lower-cased media type only (no parameters).
fn content_type_of(headers: &HeaderMap) -> String {
    headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

/// Deserialize a ragged write body into the JSON list-of-lists the adapter
/// consumes, per the request Content-Type. Mirrors Python's
/// `deserialization_registry.dispatch("ragged", media_type)`: `application/json`
/// is the list-of-lists verbatim; `application/zip` is the Awkward
/// zipped-buffers form, decoded back to a list-of-lists.
fn deserialize_ragged_body(
    content_type: &str,
    body: &[u8],
) -> Result<serde_json::Value, ServerError> {
    let media = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim();
    match media {
        "" | crate::core::media_type::mime::JSON => serde_json::from_slice(body).map_err(|e| {
            ServerError::Validation(format!(
                "ragged JSON body is not a valid list-of-lists: {e}"
            ))
        }),
        crate::core::media_type::mime::ZIP => {
            crate::serialization::ragged::from_zipped_buffers(body).map_err(|e| {
                ServerError::Validation(format!(
                    "ragged zipped-buffers body could not be decoded: {e}"
                ))
            })
        }
        other => Err(ServerError::UnsupportedMediaType(format!(
            "ragged write: unsupported Content-Type {other:?} \
             (use application/json or application/zip)"
        ))),
    }
}

/// Walk the tree to a ragged node and confirm it is writable, or map to the
/// right error: 404 if the node is not a ragged array, 405 if it is not
/// writable. Returns the read-face `Arc`; the caller re-derives the write face
/// (the resolver's `writable` flag is stable, so `as_writable()` stays `Some`).
async fn resolve_writable_ragged(
    state: &AppState,
    segments: &[String],
) -> Result<Arc<dyn crate::core::adapters::RaggedAdapterRead>, ServerError> {
    let adapter = core::walk_tree(state.root_tree.as_ref(), segments).await?;
    let ragged = adapter.as_ragged_arc().ok_or_else(|| {
        ServerError::WrongType(format!("'{}' is not a ragged array", segments.join("/")))
    })?;
    if ragged.as_writable().is_none() {
        return Err(ServerError::MethodNotAllowed(
            "this ragged node is not writable; only internally-managed ragged arrays \
             whose SQLite store is under the server's writable storage accept writes"
                .into(),
        ));
    }
    Ok(ragged)
}

/// Persist a ragged `patch`'s grown structure to the catalog and return the
/// structure to send back. Faithful to Python `CatalogRaggedAdapter.patch`
/// (catalog/adapter.py:1736-1777): only `shape` and `chunks` are written back,
/// preserving the data source's original `size`.
async fn persist_ragged_patch(
    state: &AppState,
    segments: &[String],
    new_structure: &crate::core::structures::RaggedStructure,
) -> Result<serde_json::Value, ServerError> {
    let catalog = state.catalog.as_ref().ok_or_else(|| {
        ServerError::Internal(
            "ragged patch requires a catalog to persist the grown structure".into(),
        )
    })?;
    let node = catalog
        .lookup(segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("node '{}' not found", segments.join("/"))))?;
    let data_sources = catalog
        .list_data_sources(node.id)
        .await
        .map_err(map_catalog_err)?;
    let ds = data_sources.first().ok_or_else(|| {
        ServerError::Internal(format!(
            "ragged node '{}' has no data source to update",
            segments.join("/")
        ))
    })?;

    let new_json = serde_json::to_value(new_structure)
        .map_err(|e| ServerError::Internal(format!("serialize ragged structure: {e}")))?;
    let mut persisted = ds.structure.clone();
    if let (Some(obj), Some(new_obj)) = (persisted.as_object_mut(), new_json.as_object()) {
        if let Some(shape) = new_obj.get("shape") {
            obj.insert("shape".into(), shape.clone());
        }
        if let Some(chunks) = new_obj.get("chunks") {
            obj.insert("chunks".into(), chunks.clone());
        }
    }
    catalog
        .update_data_source(ds.id, persisted.clone(), ds.parameters.clone())
        .await
        .map_err(map_catalog_err)?;
    Ok(persisted)
}

// PUT /api/v1/ragged/full/{*path} — write the whole ragged array as chunk 0.
pub async fn ragged_full_put(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/ragged/full/");
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::WriteData,
    )
    .await?;

    let persist = query_bool(&params, "persist", true);
    let ragged = resolve_writable_ragged(&state, &segments).await?;
    let writable = ragged
        .as_writable()
        .expect("resolve_writable_ragged guarantees a writable face");

    // Stream the whole-ragged write BEFORE the persist branch — upstream's
    // inherited `write` calls `_stream` ahead of `if not persist: return`
    // (catalog/adapter.py:1665-1669, dispatching the ragged `_stream` override
    // :1770), so a stream-only write still reaches subscribers. `shape` is the
    // structure's shape (variable axes `None`); the body is the raw request
    // encoding and `media_type` its Content-Type.
    let media_type = content_type_of(&headers);
    stream_ragged_data(
        &state,
        &segments,
        &media_type,
        &ragged.structure().shape,
        None,
        None,
        body.clone(),
    )
    .await;

    if persist {
        let data = deserialize_ragged_body(&media_type, &body)?;
        writable.write(&data).await.map_err(ServerError::from)?;
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

// PUT /api/v1/ragged/block/{*path}?block=i — write one chunk.
pub async fn ragged_block_put(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/ragged/block/");

    // `?block=i,j,…` is required (Python `parse_block_param`); for ragged only
    // the leftmost index (the chunk index) is meaningful.
    let block_str = params.get("block").ok_or_else(|| {
        ServerError::BadRequest("ragged block write requires a ?block= index".into())
    })?;
    let block = parse_csv_usize(block_str)?;
    let chunk_index = *block
        .first()
        .ok_or_else(|| ServerError::BadRequest("?block= must have at least one index".into()))?;

    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::WriteData,
    )
    .await?;

    let persist = query_bool(&params, "persist", true);
    let ragged = resolve_writable_ragged(&state, &segments).await?;
    let writable = ragged
        .as_writable()
        .expect("resolve_writable_ragged guarantees a writable face");

    // Stream the block write BEFORE the persist branch — upstream `write_block`
    // calls `_stream` ahead of `if not persist: return`
    // (catalog/adapter.py:1785-1795), carrying the full structure shape (ragged
    // blocks have variable axes, so no per-block shape) and the block coordinate.
    let media_type = content_type_of(&headers);
    stream_ragged_data(
        &state,
        &segments,
        &media_type,
        &ragged.structure().shape,
        None,
        Some(&block),
        body.clone(),
    )
    .await;

    if persist {
        let data = deserialize_ragged_body(&media_type, &body)?;
        writable
            .write_block(&data, chunk_index)
            .await
            .map_err(ServerError::from)?;
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

// PATCH /api/v1/ragged/full/{*path}?shape=…&offset=…&extend=…&persist=…
// Append a chunk along the leftmost dimension, growing the structure.
pub async fn ragged_full_patch(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/ragged/full/");

    let extend = query_bool(&params, "extend", false);
    let persist = query_bool(&params, "persist", true);
    // Python rejects extend=true with persist=false up front (router.py:1006).
    if extend && !persist {
        return Err(ServerError::BadRequest(
            "Cannot PATCH a ragged array with both parameters extend=true and \
             persist=false. To extend the array, you must persist the changes. \
             To skip persisting the changes, you must not extend the array."
                .into(),
        ));
    }
    // `?shape=` and `?offset=` are required query params (Python `shape_param`
    // / `offset_param`). `shape` is the incoming block's shape (streamed as the
    // `ragged-data` metadata), `offset` where it lands (drives the append).
    let shape =
        parse_csv_usize(params.get("shape").ok_or_else(|| {
            ServerError::BadRequest("ragged patch requires a ?shape= param".into())
        })?)?;
    let offset = parse_csv_usize(params.get("offset").ok_or_else(|| {
        ServerError::BadRequest("ragged patch requires an ?offset= param".into())
    })?)?;

    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::WriteData,
    )
    .await?;

    let ragged = resolve_writable_ragged(&state, &segments).await?;
    let writable = ragged
        .as_writable()
        .expect("resolve_writable_ragged guarantees a writable face");

    // Stream the incoming block BEFORE the persist branch — upstream `patch`
    // calls `_stream` ahead of `if not persist: return`
    // (catalog/adapter.py:1802-1807), so a stream-only patch still reaches
    // subscribers. The `?shape=` axes are concrete (query-param ints).
    let media_type = content_type_of(&headers);
    let shape_opt: Vec<Option<usize>> = shape.iter().map(|&d| Some(d)).collect();
    stream_ragged_data(
        &state,
        &segments,
        &media_type,
        &shape_opt,
        Some(&offset),
        None,
        body.clone(),
    )
    .await;

    // !persist: return the current structure unchanged, no write (Python
    // patch_ragged_full returns entry.structure() before deserializing).
    if !persist {
        let structure_json = serde_json::to_value(ragged.structure())
            .map_err(|e| ServerError::Internal(format!("serialize ragged structure: {e}")))?;
        return Ok(Json(structure_json));
    }

    let data = deserialize_ragged_body(&media_type, &body)?;
    let new_structure = writable
        .patch(&data, &offset, extend)
        .await
        .map_err(ServerError::from)?;
    let persisted = persist_ragged_patch(&state, &segments, &new_structure).await?;
    Ok(Json(persisted))
}

// ---------------------------------------------------------------------------
// Awkward array read+write handlers
// ---------------------------------------------------------------------------
//
// Three routes mirror Python's awkward family (router.py:1562-2310):
//
//   GET  /api/v1/awkward/full/{path}    — read ALL buffers, return ZIP
//   PUT  /api/v1/awkward/full/{path}    — receive ZIP body, write buffers
//   GET  /api/v1/awkward/buffers/{path} — read filtered buffers, return ZIP
//   POST /api/v1/awkward/buffers/{path} — same, form keys in JSON body
//
// Wire format: a ZIP archive (uncompressed, ZIP_STORED) in which each entry
// name is a buffer form key (e.g. "node0-data", "node0-offsets").  Matches
// Python `to_zipped_buffers` / `from_zipped_buffers`
// (tiled/serialization/awkward.py:14-36).

/// Pack a buffer map into an uncompressed ZIP archive.
///
/// Each entry is named by its form key.  Matches Python's `to_zipped_buffers`
/// which uses `zipfile.ZIP_STORED`.
fn pack_buffers_to_zip(
    buffers: &HashMap<String, bytes::Bytes>,
) -> Result<bytes::Bytes, ServerError> {
    use zip::write::SimpleFileOptions;

    let cursor = Cursor::new(Vec::<u8>::new());
    let mut zip = zip::ZipWriter::new(cursor);
    let opts = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (name, data) in buffers {
        zip.start_file(name, opts)
            .map_err(|e| ServerError::Internal(format!("awkward zip start_file: {e}")))?;
        zip.write_all(data)
            .map_err(|e| ServerError::Internal(format!("awkward zip write: {e}")))?;
    }
    let cursor = zip
        .finish()
        .map_err(|e| ServerError::Internal(format!("awkward zip finish: {e}")))?;
    Ok(bytes::Bytes::from(cursor.into_inner()))
}

/// Unpack a ZIP archive into a buffer map.  Used by `put_awkward_full`.
fn unpack_zip_to_buffers(data: &[u8]) -> Result<HashMap<String, bytes::Bytes>, ServerError> {
    use std::io::Read as IoRead;

    let cursor = Cursor::new(data);
    let mut archive = zip::ZipArchive::new(cursor)
        .map_err(|e| ServerError::Validation(format!("invalid ZIP body: {e}")))?;
    let mut out = HashMap::new();
    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|e| ServerError::Validation(format!("ZIP entry {i}: {e}")))?;
        let name = entry.name().to_string();
        let mut buf = Vec::new();
        entry
            .read_to_end(&mut buf)
            .map_err(|e| ServerError::Validation(format!("ZIP read {name}: {e}")))?;
        out.insert(name, bytes::Bytes::from(buf));
    }
    Ok(out)
}

/// Build the HTTP response for an awkward buffer read.
///
/// Packs `buffers` into a ZIP archive in `spawn_blocking`, negotiates the
/// media type (only `application/zip` is registered), invokes the serializer
/// (identity for ZIP), and returns the response.
async fn build_awkward_response(
    buffers: HashMap<String, bytes::Bytes>,
    structure: &crate::core::structures::AwkwardStructure,
    format_param: Option<&str>,
    headers: &HeaderMap,
    state: &AppState,
    filename: Option<&str>,
) -> Result<axum::response::Response, ServerError> {
    let nbytes: usize = buffers.values().map(|b| b.len()).sum();
    check_response_size(
        nbytes,
        state.response_bytesize_limit,
        "Use form_key filtering (\"?form_key=...\") to request a subset of buffers.",
    )?;

    let family = crate::core::structures::StructureFamily::Awkward;
    let accept = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let media_type = crate::serialization::negotiate_media_type(
        format_param,
        accept,
        family,
        &state.serialization_registry,
    )
    .ok_or_else(|| {
        unsupported_media_type(
            family,
            format_param.unwrap_or(accept),
            &state.serialization_registry,
        )
    })?;

    let ser_meta = serde_json::to_value(structure)
        .map_err(|e| ServerError::Internal(format!("awkward structure encode: {e}")))?;

    let zip_bytes = tokio::task::spawn_blocking(move || pack_buffers_to_zip(&buffers))
        .await
        .map_err(|e| ServerError::Internal(format!("blocking task failed: {e}")))??;

    let serializer = state
        .serialization_registry
        .dispatch(family, &media_type)
        .ok_or_else(|| {
            unsupported_media_type(family, &media_type, &state.serialization_registry)
        })?;

    let body = tokio::task::spawn_blocking(move || serializer(&zip_bytes, &ser_meta))
        .await
        .map_err(|e| ServerError::Internal(format!("serialize task failed: {e}")))?
        .map_err(map_serialize_error)?;

    Ok(serve_with_range(headers, &media_type, body, filename))
}

// ---------------------------------------------------------------------------
// GET /api/v1/awkward/full/{*path}
// ---------------------------------------------------------------------------

/// Read the entire awkward array — all buffers — and return them as a ZIP.
///
/// Python parity: `awkward_full` (router.py:1704-1763):
/// 1. read the array via `entry.read()`
/// 2. convert to buffers with `awkward.to_buffers(array)`
/// 3. serialize with `construct_data_response`
///
/// In Rust, `AwkwardAdapterRead::read` already returns the buffer map, so
/// step 2 is implicit.
pub async fn awkward_full(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/awkward/full/");
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::ReadData,
    )
    .await?;

    let format_str = params.get("format").cloned();
    let filename_str = params.get("filename").cloned();
    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;

    let awkward = adapter.as_awkward_arc().ok_or_else(|| {
        ServerError::WrongType(format!("'{}' is not an awkward array", segments.join("/")))
    })?;

    let buffers = awkward.read().await.map_err(ServerError::from)?;
    let structure = awkward.structure().clone();

    build_awkward_response(
        buffers,
        &structure,
        format_str.as_deref(),
        &headers,
        &state,
        filename_str.as_deref(),
    )
    .await
}

// ---------------------------------------------------------------------------
// PUT /api/v1/awkward/full/{*path}
// ---------------------------------------------------------------------------

/// Write a buffer map (as a ZIP body) to an awkward array node.
///
/// Python parity: `put_awkward_full` (router.py:2272-2310).
pub async fn put_awkward_full(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    auth: crate::server::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/awkward/full/");
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::WriteData,
    )
    .await?;

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
    let awkward = adapter.as_awkward_arc().ok_or_else(|| {
        ServerError::WrongType(format!("'{}' is not an awkward array", segments.join("/")))
    })?;

    let writable = awkward.as_writable().ok_or_else(|| {
        ServerError::MethodNotAllowed(
            "this awkward node is not writable; only internally-managed nodes accept writes".into(),
        )
    })?;

    let buffers = tokio::task::spawn_blocking(move || unpack_zip_to_buffers(&body))
        .await
        .map_err(|e| ServerError::Internal(format!("blocking task failed: {e}")))??;

    writable.write(buffers).await.map_err(ServerError::from)?;

    Ok(Json(serde_json::json!({ "ok": true })))
}

// ---------------------------------------------------------------------------
// GET /api/v1/awkward/buffers/{*path}
// ---------------------------------------------------------------------------

/// Fetch a filtered subset of awkward buffers (GET variant).
///
/// `?form_key=A&form_key=B` selects which buffers to return.  Uses
/// `Vec<(String, String)>` query extraction so repeated `?form_key=` params
/// all survive (a `HashMap` collapses them).
///
/// Python parity: `get_awkward_buffers` (router.py:1562-1608).
pub async fn awkward_buffers(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<Vec<(String, String)>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/awkward/buffers/");
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::ReadData,
    )
    .await?;

    let form_keys: Vec<String> = params
        .iter()
        .filter(|(k, _)| k == "form_key")
        .map(|(_, v)| v.clone())
        .collect();
    let format_str = params
        .iter()
        .find(|(k, _)| k == "format")
        .map(|(_, v)| v.clone());
    let filename_str = params
        .iter()
        .find(|(k, _)| k == "filename")
        .map(|(_, v)| v.clone());

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
    let awkward = adapter.as_awkward_arc().ok_or_else(|| {
        ServerError::WrongType(format!("'{}' is not an awkward array", segments.join("/")))
    })?;

    let keys_opt: Option<Vec<String>> = if form_keys.is_empty() {
        None
    } else {
        Some(form_keys)
    };

    let buffers = awkward
        .read_buffers(keys_opt.as_deref())
        .await
        .map_err(ServerError::from)?;
    let structure = awkward.structure().clone();

    build_awkward_response(
        buffers,
        &structure,
        format_str.as_deref(),
        &headers,
        &state,
        filename_str.as_deref(),
    )
    .await
}

// ---------------------------------------------------------------------------
// POST /api/v1/awkward/buffers/{*path}
// ---------------------------------------------------------------------------

/// Fetch a filtered subset of awkward buffers (POST variant).
///
/// Body: a JSON array of form keys (e.g. `["node0", "node1"]`).  POST is
/// preferred by the Rust (and Python) client because a large key set can
/// exceed URL length limits when expressed as repeated query params.
///
/// Python parity: `post_awkward_buffers` (router.py:1612-1658).
pub async fn post_awkward_buffers(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
    Json(form_keys): Json<Vec<String>>,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/awkward/buffers/");
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::ReadData,
    )
    .await?;

    let format_str = params.get("format").cloned();
    let filename_str = params.get("filename").cloned();

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
    let awkward = adapter.as_awkward_arc().ok_or_else(|| {
        ServerError::WrongType(format!("'{}' is not an awkward array", segments.join("/")))
    })?;

    let keys_opt: Option<Vec<String>> = if form_keys.is_empty() {
        None
    } else {
        Some(form_keys)
    };

    let buffers = awkward
        .read_buffers(keys_opt.as_deref())
        .await
        .map_err(ServerError::from)?;
    let structure = awkward.structure().clone();

    build_awkward_response(
        buffers,
        &structure,
        format_str.as_deref(),
        &headers,
        &state,
        filename_str.as_deref(),
    )
    .await
}

// ---------------------------------------------------------------------------
// GET /api/v1/table/partition/{*path}
// ---------------------------------------------------------------------------

pub async fn table_partition(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    // Vec<(K,V)> preserves repeated keys so ?column=A&column=B all survive.
    Query(params): Query<Vec<(String, String)>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    let segments = segments_from_uri(&uri, "/api/v1/table/partition/");
    let partition = table_partition_index(&params);

    // Collect column projection: `column` (preferred) + `field` (deprecated alias).
    // Both may be repeated: ?column=A&column=B selects columns A and B.
    // Upstream router.py:1058-1059 accepts both keys.
    let columns: Vec<String> = params
        .iter()
        .filter(|(k, _)| k == "column" || k == "field")
        .map(|(_, v)| v.clone())
        .collect();
    let fields: Option<Vec<String>> = (!columns.is_empty()).then_some(columns);
    let format_param = table_partition_format(&params);
    let filename_param = table_partition_filename(&params);

    table_partition_core(
        &state,
        &auth,
        &segments,
        partition,
        fields,
        format_param,
        filename_param,
        &headers,
    )
    .await
}

/// `POST /api/v1/table/partition/{path}` — the wide-table fallback. When a
/// column projection would overflow the GET URI, the Python client moves the
/// columns into a JSON-array body (`dataframe.py:122-133`) and the server reads
/// them from there instead of repeated `column=` params (parity with
/// `post_table_partition`, router.py:1115). `partition`/`format` stay query
/// params. An absent or empty body means "all columns".
pub async fn post_table_partition(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<Vec<(String, String)>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
    columns: Option<Json<Vec<String>>>,
) -> Result<impl IntoResponse, ServerError> {
    let segments = segments_from_uri(&uri, "/api/v1/table/partition/");
    let partition = table_partition_index(&params);
    let format_param = table_partition_format(&params);
    let filename_param = table_partition_filename(&params);
    let fields = columns.map(|Json(c)| c).filter(|c| !c.is_empty());

    table_partition_core(
        &state,
        &auth,
        &segments,
        partition,
        fields,
        format_param,
        filename_param,
        &headers,
    )
    .await
}

fn table_partition_index(params: &[(String, String)]) -> usize {
    params
        .iter()
        .find(|(k, _)| k == "partition")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0)
}

fn table_partition_format(params: &[(String, String)]) -> Option<String> {
    params
        .iter()
        .find(|(k, _)| k == "format")
        .map(|(_, v)| v.clone())
}

fn table_partition_filename(params: &[(String, String)]) -> Option<String> {
    params
        .iter()
        .find(|(k, _)| k == "filename")
        .map(|(_, v)| v.clone())
}

/// Shared read+respond core for the GET and POST `table/partition` handlers.
/// The two differ only in where the column projection comes from (repeated
/// query params vs a JSON body); auth, the per-node policy check, the tree
/// walk, the bounds check, and response negotiation are identical.
#[allow(clippy::too_many_arguments)]
async fn table_partition_core(
    state: &AppState,
    auth: &crate::server::AuthContext,
    segments: &[String],
    partition: usize,
    fields: Option<Vec<String>>,
    format_param: Option<String>,
    filename_param: Option<String>,
    headers: &HeaderMap,
) -> Result<axum::response::Response, ServerError> {
    auth.require(crate::auth::Scope::ReadData)?;
    // H2: per-node policy check.
    let _ = resolve_entry(state, auth.clone(), segments, crate::auth::Scope::ReadData).await?;

    // The async tree walk resolves each hop on the executor and hands back an
    // owned `Arc` leaf; the partition read future offloads its own blocking and
    // is awaited on the executor; the Arrow IPC encode is offloaded on its own.
    let adapter = core::walk_tree(state.root_tree.as_ref(), segments).await?;
    let table_adapter: Arc<dyn crate::core::adapters::TableAdapterRead> =
        adapter.as_table_arc().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not a table", segments.join("/")))
        })?;

    let npartitions = table_adapter.structure().npartitions;
    if partition >= npartitions {
        return Err(ServerError::BadRequest(format!(
            "Partition index {partition} out of range (table has {npartitions} partitions)"
        )));
    }

    let table = table_adapter
        .read_partition(partition, fields.as_deref())
        .await
        .map_err(ServerError::from)?;

    let metadata = table_adapter.metadata().clone();
    build_table_response(
        table,
        metadata,
        format_param.as_deref(),
        headers,
        state,
        filename_param.as_deref(),
    )
    .await
}

// ---------------------------------------------------------------------------
// GET /api/v1/table/full/{*path}
// ---------------------------------------------------------------------------
//
// Upstream router.py:1215 `get_table_full` / 1296 `table_full`. Reads the
// WHOLE table (all partitions) via `read`, with optional column projection,
// then serializes exactly like `table_partition`.

pub async fn table_full(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    // Vec<(K,V)> preserves repeated keys so ?column=A&column=B all survive.
    Query(params): Query<Vec<(String, String)>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/table/full/");
    // H2: per-node policy check.
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::ReadData,
    )
    .await?;

    // Collect column projection: `column` (preferred) + `field` (deprecated alias).
    // Both may be repeated: ?column=A&column=B selects columns A and B.
    // Upstream router.py:1058-1059 accepts both keys.
    let columns: Vec<String> = params
        .iter()
        .filter(|(k, _)| k == "column" || k == "field")
        .map(|(_, v)| v.clone())
        .collect();
    let fields: Option<Vec<String>> = if columns.is_empty() {
        None
    } else {
        Some(columns)
    };

    let format_param = params
        .iter()
        .find(|(k, _)| k == "format")
        .map(|(_, v)| v.clone());
    let filename_param = params
        .iter()
        .find(|(k, _)| k == "filename")
        .map(|(_, v)| v.clone());

    // The async tree walk resolves each hop on the executor and hands back an
    // owned `Arc` clone of the leaf; the read future offloads its own blocking
    // and is awaited on the executor below (see `table_partition`).
    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
    let table_adapter: Arc<dyn crate::core::adapters::TableAdapterRead> =
        adapter.as_table_arc().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not a table", segments.join("/")))
        })?;

    let table = table_adapter
        .read(fields.as_deref())
        .await
        .map_err(ServerError::from)?;

    let metadata = table_adapter.metadata().clone();
    build_table_response(
        table,
        metadata,
        format_param.as_deref(),
        &headers,
        &state,
        filename_param.as_deref(),
    )
    .await
}

// ---------------------------------------------------------------------------
// POST /api/v1/table/full — long-URL workaround for the GET counterpart
// ---------------------------------------------------------------------------
//
// Mirrors upstream `post_table_full` (router.py:1258). Applies when the
// column projection list grows long enough to bump into the practical URL
// length cap. Body shape (Rust port convention, path in body like
// `array_full_post`): `{path, columns?, format?}`.

#[derive(Debug, Deserialize)]
pub struct TableFullRequest {
    /// Forward-slash-separated tree path. Empty string = root.
    #[serde(default)]
    pub path: String,
    /// Column projection. `column` is accepted as an alias (the GET query key).
    #[serde(default, alias = "column")]
    pub columns: Option<Vec<String>>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub filename: Option<String>,
}

pub async fn table_full_post(
    state: State<AppState>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
    Json(req): Json<TableFullRequest>,
) -> Result<axum::response::Response, ServerError> {
    let path = req.path.trim_start_matches('/');
    let uri: axum::http::Uri = format!("/api/v1/table/full/{path}")
        .parse()
        .map_err(|e| ServerError::Internal(format!("rebuild URI: {e}")))?;

    // Rebuild the query-param list `table_full` expects, preserving each
    // projected column as its own `column=` entry.
    let mut query: Vec<(String, String)> = Vec::new();
    if let Some(cols) = &req.columns {
        for c in cols {
            query.push(("column".to_string(), c.clone()));
        }
    }
    if let Some(f) = &req.format {
        query.push(("format".to_string(), f.clone()));
    }
    if let Some(name) = &req.filename {
        query.push(("filename".to_string(), name.clone()));
    }

    table_full(state, OriginalUri(uri), Query(query), headers, auth)
        .await
        .map(IntoResponse::into_response)
}

// ---------------------------------------------------------------------------
// GET /api/v1/node/full/{*path} — deprecated family-agnostic alias
// ---------------------------------------------------------------------------
//
// Mirrors upstream `node_full` (router.py:1477-1559, `deprecated=True` since
// tiled commit c7edd9d "Deprecate /node/full/{path} routes", Nov 2023).
// Upstream keeps serving it for old-client back-compat; this dispatches to
// the already-implemented `table_full`/`container_full` core by resolving
// the target node's structure family and delegating, the same
// call-the-real-handler-with-a-rewritten-URI pattern `table_full_post` uses
// above. `field=` (Python's query key on this route) reaches `table_full`
// unchanged — it already treats `field` as a `column` alias (see
// `table_full`'s "Collect column projection" comment); `container_full` has
// no field-based child filtering in this port either, so the container
// branch has the same scope as Python's non-deprecated `/container/full`.
/// Root-path variant (`GET /api/v1/node/full/`, no trailing segments). Axum's
/// `{*path}` wildcard does not match a zero-segment path — `container_full`
/// needs the identical split (`container_full_root` + `container_full`), so
/// `node_full` gets one too.
pub async fn node_full_root(
    state: State<AppState>,
    base_url: BaseUrl,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
) -> Result<axum::response::Response, ServerError> {
    node_full(
        state,
        OriginalUri("/api/v1/node/full/".parse().expect("static URI")),
        base_url,
        Query(params),
        headers,
        auth,
    )
    .await
}

pub async fn node_full(
    state: State<AppState>,
    OriginalUri(uri): OriginalUri,
    base_url: BaseUrl,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
) -> Result<axum::response::Response, ServerError> {
    let segments = segments_from_uri(&uri, "/api/v1/node/full/");
    let family = if segments.is_empty() {
        // The root is always a container (Python: entry.structure_family is
        // never checked for the empty path here since get_entry would have
        // already resolved the root tree adapter).
        crate::core::structures::StructureFamily::Container
    } else {
        core::walk_tree(state.0.root_tree.as_ref(), &segments)
            .await?
            .structure_family()
    };
    let path = segments.join("/");
    match family {
        crate::core::structures::StructureFamily::Table => {
            let uri: axum::http::Uri = format!("/api/v1/table/full/{path}")
                .parse()
                .map_err(|e| ServerError::Internal(format!("rebuild URI: {e}")))?;
            let query: Vec<(String, String)> = params.into_iter().collect();
            table_full(state, OriginalUri(uri), Query(query), headers, auth)
                .await
                .map(IntoResponse::into_response)
        }
        crate::core::structures::StructureFamily::Container => {
            let uri: axum::http::Uri = format!("/api/v1/container/full/{path}")
                .parse()
                .map_err(|e| ServerError::Internal(format!("rebuild URI: {e}")))?;
            container_full(
                state,
                OriginalUri(uri),
                base_url,
                Query(params),
                headers,
                auth,
            )
            .await
            .map(IntoResponse::into_response)
        }
        other => Err(ServerError::WrongType(format!(
            "'{path}' is a {other:?}, not a table or container"
        ))),
    }
}

/// `PUT /api/v1/node/full/{*path}` — deprecated alias of `PUT /table/full`
/// (Python decorates the single `put_node_full` function with both paths,
/// router.py:2161-2162 — there is no container-write branch upstream either,
/// since containers have no whole-node write). `table_full_put` extracts its
/// path segments by locating the literal `/api/v1/table/full/` substring in
/// the request URI (`segments_from_uri`), so it cannot be mounted directly on
/// this route — the URI must be rewritten first, same as the GET dispatcher
/// above.
pub async fn node_full_put(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    auth: crate::server::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let segments = segments_from_uri(&uri, "/api/v1/node/full/");
    let path = segments.join("/");
    let rewritten: axum::http::Uri = format!("/api/v1/table/full/{path}")
        .parse()
        .map_err(|e| ServerError::Internal(format!("rebuild URI: {e}")))?;
    table_full_put(State(state), OriginalUri(rewritten), auth, body).await
}

// ---------------------------------------------------------------------------
// PUT /api/v1/table/full/{*path} — overwrite a writable table's data
// ---------------------------------------------------------------------------
//
// The write counterpart of `GET /table/full`. The body is an Arrow IPC FILE
// stream (the canonical table interchange the read serializers also consume),
// decoded into an `ArrowTable` and written whole. Only internally-managed
// tables whose backing file lives under the server's writable storage are
// writable — the resolver decides this, so a non-writable node answers 405
// rather than the route silently not existing. Mirrors Python tiled's
// `PUT /table/full` (router.py put_node_full), scoped to the whole-table case
// the CSV backend supports.
pub async fn table_full_put(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    auth: crate::server::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/table/full/");
    // Per-node policy check, same as every other data handler.
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::WriteData,
    )
    .await?;

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
    let table_adapter: Arc<dyn crate::core::adapters::TableAdapterRead> =
        adapter.as_table_arc().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not a table", segments.join("/")))
        })?;
    let writable = table_adapter.as_table_writable().ok_or_else(|| {
        ServerError::MethodNotAllowed(
            "this table node is not writable; only internally-managed tables under \
             the server's writable storage accept writes"
                .into(),
        )
    })?;

    let table = decode_arrow_ipc_table(&body)?;
    validate_table_columns(&table, table_adapter.structure())?;

    writable.write(table).await.map_err(ServerError::from)?;

    // Stream the whole-table write on the table node's own stream. The wire
    // encoding is Arrow IPC; `partition=None`/`append=false` mark a full replace.
    stream_table_data(
        &state,
        &segments,
        crate::core::media_type::mime::ARROW_FILE,
        None,
        false,
        body,
    )
    .await;

    Ok(Json(serde_json::json!({ "ok": true })))
}

/// Validate that `table`'s column names match the node's declared columns.
/// Returns a 422 error when they disagree, preventing a later read from
/// encountering a schema that contradicts the catalog structure.
fn validate_table_columns(
    table: &crate::core::dtype::ArrowTable,
    structure: &crate::core::structures::TableStructure,
) -> Result<(), ServerError> {
    let declared = &structure.columns;
    let incoming: Vec<String> = table
        .schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    if &incoming != declared {
        return Err(ServerError::Validation(format!(
            "table write columns {incoming:?} do not match the node's columns {declared:?}"
        )));
    }
    Ok(())
}

/// Decode an Arrow IPC FILE stream into an [`ArrowTable`]. This is the canonical
/// table write interchange — the same IPC the read serializers consume.
fn decode_arrow_ipc_table(body: &[u8]) -> Result<crate::core::dtype::ArrowTable, ServerError> {
    use arrow::ipc::reader::FileReader;
    let cursor = std::io::Cursor::new(body.to_vec());
    let reader = FileReader::try_new(cursor, None)
        .map_err(|e| ServerError::Validation(format!("invalid Arrow IPC table body: {e}")))?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    for b in reader {
        batches.push(b.map_err(|e| ServerError::Validation(format!("Arrow IPC batch: {e}")))?);
    }
    Ok(crate::core::dtype::ArrowTable { batches, schema })
}

// ---------------------------------------------------------------------------
// PUT /api/v1/table/partition/{*path} — overwrite one partition
// ---------------------------------------------------------------------------
//
// Mirrors Python tiled `put_table_partition` (router.py:2194-2231). The body
// is an Arrow IPC FILE stream. The `partition` index is read from the query
// string (same key as the GET handler). The adapter's `write_partition` method
// replaces the data for that partition index; a CSV/Parquet adapter that is
// single-partition rejects any index other than 0.
pub async fn table_partition_put(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<Vec<(String, String)>>,
    auth: crate::server::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/table/partition/");
    let partition = table_partition_index(&params);
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::WriteData,
    )
    .await?;

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
    let table_adapter: Arc<dyn crate::core::adapters::TableAdapterRead> =
        adapter.as_table_arc().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not a table", segments.join("/")))
        })?;
    let writable = table_adapter.as_table_writable().ok_or_else(|| {
        ServerError::MethodNotAllowed(
            "this table node is not writable; only internally-managed tables under \
             the server's writable storage accept writes"
                .into(),
        )
    })?;

    let table = decode_arrow_ipc_table(&body)?;
    validate_table_columns(&table, table_adapter.structure())?;

    let npartitions = table_adapter.structure().npartitions;
    if partition >= npartitions {
        return Err(ServerError::BadRequest(format!(
            "Partition index {partition} out of range (table has {npartitions} partitions)"
        )));
    }

    writable
        .write_partition(table, partition)
        .await
        .map_err(ServerError::from)?;

    // Stream the partition replace (Arrow IPC; `append=false`).
    stream_table_data(
        &state,
        &segments,
        crate::core::media_type::mime::ARROW_FILE,
        Some(partition),
        false,
        body,
    )
    .await;

    Ok(Json(serde_json::json!({ "ok": true })))
}

// ---------------------------------------------------------------------------
// PATCH /api/v1/table/partition/{*path} — append rows to one partition
// ---------------------------------------------------------------------------
//
// Mirrors Python tiled `patch_table_partition` (router.py:2233-2270). The body
// is an Arrow IPC FILE stream whose rows are appended to the existing partition
// data. The `partition` index is read from the query string.
pub async fn table_partition_patch(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<Vec<(String, String)>>,
    auth: crate::server::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/table/partition/");
    let partition = table_partition_index(&params);
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::WriteData,
    )
    .await?;

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
    let table_adapter: Arc<dyn crate::core::adapters::TableAdapterRead> =
        adapter.as_table_arc().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not a table", segments.join("/")))
        })?;
    let writable = table_adapter.as_table_writable().ok_or_else(|| {
        ServerError::MethodNotAllowed(
            "this table node is not writable; only internally-managed tables under \
             the server's writable storage accept writes"
                .into(),
        )
    })?;

    let table = decode_arrow_ipc_table(&body)?;
    validate_table_columns(&table, table_adapter.structure())?;

    let npartitions = table_adapter.structure().npartitions;
    if partition >= npartitions {
        return Err(ServerError::BadRequest(format!(
            "Partition index {partition} out of range (table has {npartitions} partitions)"
        )));
    }

    writable
        .append_partition(table, partition)
        .await
        .map_err(ServerError::from)?;

    // Stream the partition append (Arrow IPC; `append=true` — the append flag is
    // set only on this PATCH path, distinguishing it from a PUT replace).
    stream_table_data(
        &state,
        &segments,
        crate::core::media_type::mime::ARROW_FILE,
        Some(partition),
        true,
        body,
    )
    .await;

    Ok(Json(serde_json::json!({ "ok": true })))
}

// ---------------------------------------------------------------------------
// GET /documents/{*path} — Stream Bluesky documents (databroker compat)
// ---------------------------------------------------------------------------

pub async fn get_documents(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/documents/");
    if segments.is_empty() {
        return Err(ServerError::Validation(
            "Path to a BlueskyRun is required".into(),
        ));
    }
    // H2: per-node policy check.
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        crate::auth::Scope::ReadData,
    )
    .await?;

    // The async tree walk resolves each hop on the executor; the container's
    // `keys`/`get` offload their own blocking backend (Mongo sync driver)
    // internally, so they are awaited here without parking the executor.
    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;

    // The run must be a container (BlueskyRun).
    let run: &dyn ContainerAdapter = adapter
        .as_container()
        .ok_or_else(|| ServerError::WrongType("This is not a BlueskyRun".into()))?;

    // Build a JSON-seq response with the run's metadata as documents.
    // Format: {"name": "start", "doc": {...}}\n{"name": "stop", "doc": {...}}\n
    let meta = run.metadata();
    let mut lines = Vec::new();

    // Emit start document.
    if let Some(start) = meta.get("start") {
        let line = serde_json::json!({"name": "start", "doc": start});
        lines.push(serde_json::to_string(&line).unwrap_or_default());
    }

    // Emit descriptor documents from each stream.
    for stream_key in run.keys().await? {
        if let Some(AnyAdapter::Container(stream)) = run.get(&stream_key).await? {
            let stream_meta = stream.metadata();
            if let Some(descriptors) = stream_meta.get("descriptors")
                && let Some(arr) = descriptors.as_array()
            {
                for desc in arr {
                    let line = serde_json::json!({"name": "descriptor", "doc": desc});
                    lines.push(serde_json::to_string(&line).unwrap_or_default());
                }
            }
        }
    }

    // Emit stop document.
    if let Some(stop) = meta.get("stop")
        && !stop.is_null()
    {
        let line = serde_json::json!({"name": "stop", "doc": stop});
        lines.push(serde_json::to_string(&line).unwrap_or_default());
    }

    let body = lines.join("\n") + "\n";

    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "application/json-seq".to_string(),
        )],
        body,
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// POST /api/v1/register/{*path} — Accept registration payloads
// ---------------------------------------------------------------------------
//
// The server currently has no mutable backing store, so this handler is
// accept-only: it parses the body to validate shape and returns a synthetic
// PostMetadataResponse. Production deployments wiring up a real catalog
// (sqlite, postgres) will replace this with an implementation that actually
// persists the node.

pub async fn register_root(
    state: State<AppState>,
    base_url: BaseUrl,
    auth: crate::server::AuthContext,
    body: Json<crate::core::schemas::PostMetadataRequest>,
) -> Result<impl IntoResponse, ServerError> {
    register(
        state,
        OriginalUri("/api/v1/register/".parse().expect("static URI")),
        base_url,
        auth,
        body,
    )
    .await
}

pub async fn register(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    BaseUrl(base_url): BaseUrl,
    auth: crate::server::AuthContext,
    Json(req): Json<crate::core::schemas::PostMetadataRequest>,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::WriteMetadata)?;
    auth.require(crate::auth::Scope::CreateNode)?;
    auth.require(crate::auth::Scope::Register)?;
    let segments = create_segments_from_uri(&uri);
    // Register trusts the client-supplied assets (existing data); it does not
    // generate storage.
    create_node_core(state, segments, base_url, auth, req, false).await
}

// POST /api/v1/metadata/ — root variant of the asset-free creation alias.
pub async fn post_metadata_root(
    state: State<AppState>,
    base_url: BaseUrl,
    auth: crate::server::AuthContext,
    body: Json<crate::core::schemas::PostMetadataRequest>,
) -> Result<impl IntoResponse, ServerError> {
    post_metadata(
        state,
        OriginalUri("/api/v1/metadata/".parse().expect("static URI")),
        base_url,
        auth,
        body,
    )
    .await
}

// POST /api/v1/metadata/{*path} — asset-free node creation (Python parity:
// post_metadata, router.py:1769-1814). Shares the create core with /register/
// but, unlike it, does NOT require the `register` scope and REJECTS
// externally-managed assets, directing such requests to POST /register/{path}.
pub async fn post_metadata(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    BaseUrl(base_url): BaseUrl,
    auth: crate::server::AuthContext,
    Json(req): Json<crate::core::schemas::PostMetadataRequest>,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::WriteMetadata)?;
    auth.require(crate::auth::Scope::CreateNode)?;
    // Python rejects externally-managed assets on this endpoint
    // (router.py:1794-1799); they must go through POST /register/{path}.
    if req.data_sources.iter().any(|ds| !ds.assets.is_empty()) {
        return Err(ServerError::BadRequest(
            "Externally-managed assets cannot be registered using POST \
             /metadata/{path}. Use POST /register/{path} instead."
                .into(),
        ));
    }
    let segments = create_segments_from_uri(&uri);
    // Create generates managed storage server-side (init_storage) when
    // writable storage is configured.
    create_node_core(state, segments, base_url, auth, req, true).await
}

/// Hand a tree event to the webhook dispatcher when webhooks are enabled.
/// Called alongside the streaming-cache publish at each catalog write site — the
/// upstream shape where the write site independently notifies WS subscribers
/// (via the streaming cache) and dispatches webhooks
/// (`tiled/catalog/adapter.py:877/1360`). `event_type` is the wire event name;
/// `data` is the webhook-specific event body (its own `"type"` tag agrees with
/// `event_type`) — kept identical to the pre-PR2b payload so webhook delivery
/// semantics are unchanged. Webhook matching is purely `path`-based; `node_id`
/// is carried for delivery correlation only, not used to widen matching.
async fn dispatch_webhook_event(
    state: &AppState,
    event_type: &'static str,
    node_id: i64,
    path: &str,
    data: serde_json::Value,
) {
    if let Some(dispatcher) = &state.webhook_dispatcher {
        dispatcher
            .dispatch(event_type, node_id, path.to_string(), data)
            .await;
    }
}

/// Build the `metadata-updated` webhook body, byte-identical to the pre-PR2b
/// `UpdateKind::MetadataUpdated` serialization: `{type, metadata, specs}` with
/// `specs` omitted when it is JSON null (upstream tiled PR #1176 publishes
/// `specs` alongside `metadata`). Shared by PATCH and PUT metadata.
fn metadata_updated_webhook_data(
    metadata: &serde_json::Value,
    specs: &serde_json::Value,
) -> serde_json::Value {
    let mut data = serde_json::json!({
        "type": "metadata-updated",
        "metadata": metadata.clone(),
    });
    if !specs.is_null() {
        data["specs"] = specs.clone();
    }
    data
}

/// Shared node-creation core for `POST /register/{path}` and
/// `POST /metadata/{path}` (Python `_create_node`, router.py:1852). Callers
/// apply their own scope/asset gating before delegating here.
///
/// `generate_storage` selects the asset-handling mode: `false` (register)
/// trusts the client's assets for already-existing data; `true` (metadata
/// create) lets the server generate managed storage via `init_storage` when
/// writable storage is configured.
async fn create_node_core(
    state: AppState,
    segments: Vec<String>,
    base_url: String,
    auth: crate::server::AuthContext,
    req: crate::core::schemas::PostMetadataRequest,
    generate_storage: bool,
) -> Result<impl IntoResponse, ServerError> {
    let path = segments.join("/");
    // Prefer the top-level `id` (Python tiled wire format: server/schemas.py:462
    // `PostMetadataRequest.id`, accepts legacy `key` via serde alias), fall
    // back to `metadata.key` for older clients, then generate one.
    // Python parity: `Context.key_maker` defaults to `str(uuid.uuid4())`
    // (tiled/catalog/adapter.py:188), applied when the client omits the key
    // (router.py:1875: `key = body.id or entry.context.key_maker()`).
    let id = req
        .id
        .clone()
        .or_else(|| {
            req.metadata
                .get("key")
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let structure_family = match req.structure_family {
        crate::core::structures::StructureFamily::Container => "container",
        crate::core::structures::StructureFamily::Array => "array",
        crate::core::structures::StructureFamily::Table => "table",
        crate::core::structures::StructureFamily::Sparse => "sparse",
        crate::core::structures::StructureFamily::Awkward => "awkward",
        crate::core::structures::StructureFamily::Ragged => "ragged",
    }
    .to_string();

    if let Some(ref catalog) = state.catalog {
        // Per-ancestor auth gate on the parent container path.
        let auth = if !segments.is_empty() {
            resolve_entry(&state, auth, &segments, crate::auth::Scope::CreateNode).await?
        } else {
            auth
        };

        // Resolve parent_id by walking the segments. Empty segments → root.
        let parent_id = if segments.is_empty() {
            None
        } else {
            let parent = catalog
                .lookup(&segments)
                .await
                .map_err(map_catalog_err)?
                .ok_or_else(|| {
                    ServerError::NotFound(format!("parent path '{path}' does not exist"))
                })?;
            Some(parent.id)
        };

        // Compute the initial access_blob. The client may supply one in the
        // request; otherwise derive it from the creator principal. Then let
        // the policy validate/rewrite it via init_node before storing.
        let initial_blob = req
            .access_blob
            .clone()
            .unwrap_or_else(|| creator_access_blob(auth.principal.as_deref()));
        let final_access_blob = if let (Some(policy), Some(principal)) =
            (state.access_policy.as_deref(), auth.principal.as_deref())
        {
            let (_, blob) = policy
                .init_node(
                    principal,
                    None, // authn_access_tags: populated in sub-part 3
                    &auth.scopes,
                    Some(&initial_blob),
                )
                .await
                .map_err(ServerError::Validation)?;
            blob
        } else {
            initial_blob
        };

        let node = catalog
            .create_node(
                parent_id,
                segments.clone(),
                crate::catalog::node::RegisterRequest {
                    key: id.clone(),
                    structure_family: structure_family.clone(),
                    metadata: req.metadata.clone(),
                    specs: serde_json::to_value(&req.specs).unwrap_or_default(),
                    access_blob: final_access_blob,
                },
            )
            .await
            .map_err(map_catalog_err)?;

        // Persist any data sources sent with the create request, capturing the
        // DB-assigned primary key of each in creation order so the child-created
        // stream event below can carry them (upstream adapter.py:847-855).
        let mut persisted_ds_ids: Vec<i64> = Vec::with_capacity(req.data_sources.len());
        for ds in &req.data_sources {
            // Two creation modes share this core:
            //  * `/register` (generate_storage=false): the client supplies the
            //    assets for already-existing data. A managed asset's `file://`
            //    `data_uri` must resolve under the configured storage dirs (S2
            //    write-time containment), so it is validated here; external
            //    assets are read-only references the read resolver guards. Only
            //    managed assets become physical-delete targets, matching the
            //    delete path's `management <> 'external'` filter.
            //  * `/metadata` create (generate_storage=true): the SERVER decides
            //    where managed data lives. When writable storage is configured,
            //    a non-external data source's storage is *generated* via
            //    `init_storage` (URI + skeleton), replacing client input so the
            //    managed `data_uri` can never be client-controlled. Without
            //    writable storage configured, create falls back to the
            //    register-style persistence (no generation).
            let generate = generate_storage
                && !catalog.writable_storage().is_empty()
                && ds.management != crate::core::data_source::Management::External;
            // `parameters` mirrors what the data source advertises: for the
            // register/external path it is the client-supplied value verbatim;
            // for a server-generated managed create it is whatever
            // `managed_init_storage` returns — most backends pass the client
            // parameters through unchanged, but the ragged-SQL backend injects
            // the server-chosen `table_name`/`dataset_id` the resolver needs.
            let (mimetype, assets, parameters) = if generate {
                managed_init_storage(catalog, ds, &segments, &node.key).await?
            } else {
                if !generate_storage
                    && ds.management != crate::core::data_source::Management::External
                {
                    for a in &ds.assets {
                        catalog
                            .validate_managed_data_uri(&a.data_uri)
                            .map_err(map_catalog_err)?;
                    }
                }
                let assets: Vec<crate::catalog::data_source::AssetSpec> = ds
                    .assets
                    .iter()
                    .map(|a| crate::catalog::data_source::AssetSpec {
                        data_uri: a.data_uri.clone(),
                        is_directory: a.is_directory,
                        parameter: a.parameter.clone().unwrap_or_else(|| "data_uri".into()),
                        num: a.num.map(|n| n as i32),
                    })
                    .collect();
                (
                    ds.mimetype.clone().unwrap_or_default(),
                    assets,
                    ds.parameters.clone(),
                )
            };
            let structure_json = ds
                .structure
                .as_ref()
                .and_then(|s| serde_json::to_value(s).ok())
                .unwrap_or_default();
            let spec = crate::catalog::data_source::DataSourceSpec {
                structure_family: ds_family_str(ds.structure_family).to_string(),
                structure: structure_json,
                mimetype,
                parameters,
                management: format!("{:?}", ds.management).to_lowercase(),
                assets,
            };
            let created = catalog
                .create_data_source(node.id, spec)
                .await
                .map_err(map_catalog_err)?;
            persisted_ds_ids.push(created.id);
        }

        let child_path = if path.is_empty() {
            node.key.clone()
        } else {
            format!("{path}/{}", node.key)
        };
        // Publish a `container-child-created` event on the *parent* node's
        // stream so a subscriber watching the container learns of the new child
        // (upstream `adapter.py:858-873`), enriched with the child's specs /
        // metadata / data_sources / access_blob. The cache is node_id-keyed —
        // there is no ancestor fan-out (D4), and a root-level create (no parent
        // node id) does not stream, since the root has no subscribable node id.
        if let Some(parent_id) = parent_id {
            // Stamp each streamed data source with its DB-assigned primary key
            // (upstream adapter.py:848-855: `ds = data_source.model_copy(); ds.id
            // = data_source_orm.id`). The request objects carry no id; the
            // persisted rows do, matched here by creation order.
            let data_sources_with_ids: Vec<crate::core::data_source::DataSource> = req
                .data_sources
                .iter()
                .zip(&persisted_ds_ids)
                .map(|(ds, &id)| crate::core::data_source::DataSource {
                    id: Some(id),
                    ..ds.clone()
                })
                .collect();
            let seq = state.streaming_cache.incr_seq(parent_id).await;
            state
                .streaming_cache
                .set(
                    parent_id,
                    seq,
                    crate::server::streaming_cache::StreamEvent::child_created(
                        seq,
                        &node.key,
                        &structure_family,
                        node.specs.clone(),
                        node.metadata.clone(),
                        serde_json::to_value(&data_sources_with_ids).unwrap_or_default(),
                        node.access_blob.clone(),
                    ),
                )
                .await;
        }
        // Webhooks fire on the *parent* path (matching the parent + ancestors);
        // the body is the pre-PR2b `{type, key, structure_family}`, unchanged.
        dispatch_webhook_event(
            &state,
            "child-created",
            node.id,
            &path,
            serde_json::json!({
                "type": "child-created",
                "key": node.key.clone(),
                "structure_family": structure_family.clone(),
            }),
        )
        .await;
        let links =
            crate::core::links::links_for_node(req.structure_family, &base_url, &child_path);
        let resp = crate::core::schemas::PostMetadataResponse {
            id: node.key,
            links: Some(serde_json::to_value(&links).unwrap_or_default()),
            metadata: Some(node.metadata),
            data_sources: Some(req.data_sources),
            access_blob: Some(node.access_blob),
        };
        return Ok((axum::http::StatusCode::CREATED, Json(resp)));
    }

    // No catalog wired — accept-only fallback (synthetic id, no persistence).
    // Useful for development against a Mongo-backed read tree.
    let child_path = if path.is_empty() {
        id.clone()
    } else {
        format!("{path}/{id}")
    };
    let links = crate::core::links::links_for_node(req.structure_family, &base_url, &child_path);
    let resp = crate::core::schemas::PostMetadataResponse {
        id,
        links: Some(serde_json::to_value(&links).unwrap_or_default()),
        metadata: Some(req.metadata),
        data_sources: Some(req.data_sources),
        access_blob: None,
    };
    Ok((axum::http::StatusCode::CREATED, Json(resp)))
}

/// Default access_blob for a newly registered node: owned by the creating
/// principal so the creator can always manage their own node. Anonymous
/// creates get an empty blob (untagged = world-readable with PassthroughPolicy).
fn creator_access_blob(principal: Option<&crate::auth::Principal>) -> serde_json::Value {
    match principal {
        Some(p) => serde_json::json!({"user": p.uuid}),
        None => serde_json::Value::Object(Default::default()),
    }
}

fn ds_family_str(f: crate::core::structures::StructureFamily) -> &'static str {
    use crate::core::structures::StructureFamily as SF;
    match f {
        SF::Container => "container",
        SF::Array => "array",
        SF::Table => "table",
        SF::Sparse => "sparse",
        SF::Awkward => "awkward",
        SF::Ragged => "ragged",
    }
}

/// The mimetype the server creates by default for a structure family when the
/// client does not pin one — the port's `DEFAULT_CREATION_MIMETYPE`.
///
/// Python tiled maps array→ZARR (`adapters/__init__.py`). This port matches
/// that parity default when the zarr writer is built in (`zarr-adapter`
/// feature), and otherwise falls back to its always-available NPY writer. Both
/// are explicit mimetypes a client can still pin directly; every other family
/// is rejected (415) until its writer lands.
fn default_creation_mimetype(
    family: crate::core::structures::StructureFamily,
) -> Result<&'static str, ServerError> {
    use crate::core::structures::StructureFamily as SF;
    match family {
        #[cfg(feature = "zarr-adapter")]
        SF::Array => Ok("application/x-zarr"),
        #[cfg(not(feature = "zarr-adapter"))]
        SF::Array => Ok("application/x-npy"),
        // Tables prefer parquet when its writer is built in (parity default),
        // else CSV — the table writer this build ships by default (csv-adapter).
        #[cfg(feature = "parquet-adapter")]
        SF::Table => Ok("application/x-parquet"),
        #[cfg(all(not(feature = "parquet-adapter"), feature = "csv-adapter"))]
        SF::Table => Ok("text/csv"),
        // Ragged nodes have one managed-write backend: SQL-backed storage.
        #[cfg(feature = "sql-adapter")]
        SF::Ragged => Ok(crate::core::media_type::mime::RAGGED_SQL),
        // Awkward nodes have one managed-write backend: a directory of buffer
        // files. Upstream `DEFAULT_CREATION_MIMETYPE[awkward]`
        // (tiled/catalog/adapter.py:120) → `AWKWARD_BUFFERS_MIMETYPE`.
        SF::Awkward => Ok(crate::core::media_type::mime::AWKWARD_BUFFERS),
        // Sparse nodes have one managed-write backend: a directory of per-block
        // parquet files. Upstream `DEFAULT_CREATION_MIMETYPE[sparse]`
        // (tiled/catalog/adapter.py:122) → `SPARSE_BLOCKS_PARQUET_MIMETYPE`.
        #[cfg(feature = "parquet")]
        SF::Sparse => Ok(crate::core::media_type::mime::SPARSE_BLOCKS_PARQUET),
        other => Err(ServerError::UnsupportedMediaType(format!(
            "no managed-write backend for {} nodes in this build \
             (array: application/x-zarr or application/x-npy; \
             table: application/x-parquet or text/csv; \
             awkward: application/x-awkward-buffers; \
             sparse: application/x-parquet;structure=sparse)",
            ds_family_str(other)
        ))),
    }
}

/// Generate managed storage for a `/metadata` create: pick the write mimetype,
/// dispatch to the matching `init_storage`, and return the chosen mimetype plus
/// the server-generated assets (replacing any client input). The single place
/// that turns a managed create into on-disk storage, so the generated
/// `data_uri` is always under writable storage by construction.
///
/// Only `Writable` management is supported here: `Locked`/`Immutable` would be
/// created under writable storage yet must stay read-only, which this build's
/// containment (writable ⟺ under writable storage) cannot yet express, so they
/// are refused rather than created writable-by-accident.
async fn managed_init_storage(
    catalog: &crate::catalog::Catalog,
    ds: &crate::core::data_source::DataSource,
    parent_segments: &[String],
    key: &str,
) -> Result<
    (
        String,
        Vec<crate::catalog::data_source::AssetSpec>,
        serde_json::Value,
    ),
    ServerError,
> {
    use crate::core::data_source::Management;

    if ds.management != Management::Writable {
        return Err(ServerError::UnsupportedMediaType(format!(
            "creating {}-managed data is not supported in this build; use \
             `writable` (server-generated storage) or `external` (POST /register \
             with an existing data_uri)",
            format!("{:?}", ds.management).to_lowercase()
        )));
    }

    let writable_root = catalog
        .writable_storage()
        .first()
        .ok_or_else(|| ServerError::Internal("writable storage vanished".into()))?;

    // Choose the write mimetype: honour a client-pinned one, else the family
    // default. Only mimetypes this port can write are accepted.
    let mimetype = match ds.mimetype.as_deref() {
        Some(m) => m.to_string(),
        None => default_creation_mimetype(ds.structure_family)?.to_string(),
    };

    // The node's full path (ancestors + key); `init_storage` turns each part
    // into one on-disk path component.
    let mut path_parts: Vec<String> = parent_segments.to_vec();
    path_parts.push(key.to_string());

    // Most backends persist the client-supplied parameters verbatim; the
    // ragged-SQL arm replaces this with its server-generated SQL coordinates.
    let parameters = ds.parameters.clone();

    match mimetype.as_str() {
        "application/x-npy" | "application/x-numpy" | "npy" => {
            let structure = managed_array_structure(ds, &mimetype)?;
            let (_data_uri, assets) =
                crate::adapters::init_storage_npy(writable_root, &path_parts, &structure)
                    .map_err(ServerError::from)?;
            Ok((mimetype, to_asset_specs(assets), parameters))
        }
        "application/x-zarr" => {
            #[cfg(feature = "zarr-adapter")]
            {
                let structure = managed_array_structure(ds, &mimetype)?;
                let (_data_uri, assets) =
                    crate::adapters::init_storage_zarr(writable_root, &path_parts, &structure)
                        .map_err(ServerError::from)?;
                Ok((mimetype, to_asset_specs(assets), parameters))
            }
            #[cfg(not(feature = "zarr-adapter"))]
            {
                Err(ServerError::UnsupportedMediaType(
                    "zarr support not built in".into(),
                ))
            }
        }
        "text/csv" => {
            #[cfg(feature = "csv-adapter")]
            {
                let structure = managed_table_structure(ds, &mimetype)?;
                let (_data_uri, assets) =
                    crate::adapters::init_storage_csv(writable_root, &path_parts, &structure)
                        .map_err(ServerError::from)?;
                Ok((mimetype, to_asset_specs(assets), parameters))
            }
            #[cfg(not(feature = "csv-adapter"))]
            {
                Err(ServerError::UnsupportedMediaType(
                    "csv support not built in".into(),
                ))
            }
        }
        "application/x-parquet" => {
            #[cfg(feature = "parquet-adapter")]
            {
                let structure = managed_table_structure(ds, &mimetype)?;
                let (_data_uri, assets) =
                    crate::adapters::init_storage_parquet(writable_root, &path_parts, &structure)
                        .map_err(ServerError::from)?;
                Ok((mimetype, to_asset_specs(assets), parameters))
            }
            #[cfg(not(feature = "parquet-adapter"))]
            {
                Err(ServerError::UnsupportedMediaType(
                    "parquet support not built in".into(),
                ))
            }
        }
        crate::core::media_type::mime::RAGGED_SQL => {
            #[cfg(feature = "sql-adapter")]
            {
                let structure = managed_ragged_structure(ds, &mimetype)?;
                let init = crate::adapters::init_storage_ragged_sql(
                    writable_root,
                    &path_parts,
                    &structure,
                )
                .await
                .map_err(ServerError::from)?;
                // The resolver rebuilds the adapter from these SQL coordinates;
                // merge them onto any client parameters (server values win).
                let mut params = match parameters {
                    serde_json::Value::Object(map) => map,
                    _ => serde_json::Map::new(),
                };
                params.insert("table_name".into(), init.table_name.clone().into());
                params.insert("dataset_id".into(), init.dataset_id.into());
                Ok((
                    mimetype,
                    to_asset_specs(init.assets),
                    serde_json::Value::Object(params),
                ))
            }
            #[cfg(not(feature = "sql-adapter"))]
            {
                Err(ServerError::UnsupportedMediaType(
                    "ragged-SQL support not built in".into(),
                ))
            }
        }
        crate::core::media_type::mime::AWKWARD_BUFFERS => {
            // Validate the create carries an awkward structure (fast-fail before
            // touching disk); the structure itself is persisted verbatim in the
            // catalog data_source — the buffer directory holds only raw buffers,
            // so `init_storage_awkward` needs no structure (upstream
            // `AwkwardBuffersAdapter.init_storage`, awkward.py:120-138).
            let _structure = managed_awkward_structure(ds, &mimetype)?;
            let (_data_uri, assets) =
                crate::adapters::init_storage_awkward(writable_root, &path_parts)
                    .map_err(ServerError::from)?;
            Ok((mimetype, to_asset_specs(assets), parameters))
        }
        crate::core::media_type::mime::SPARSE_BLOCKS_PARQUET => {
            #[cfg(feature = "parquet")]
            {
                let structure = managed_sparse_structure(ds, &mimetype)?;
                let (_data_uri, assets) = crate::adapters::init_storage_sparse_parquet(
                    writable_root,
                    &path_parts,
                    &structure,
                )
                .map_err(ServerError::from)?;
                Ok((mimetype, to_asset_specs(assets), parameters))
            }
            #[cfg(not(feature = "parquet"))]
            {
                Err(ServerError::UnsupportedMediaType(
                    "sparse (parquet blocks) support not built in".into(),
                ))
            }
        }
        other => Err(ServerError::UnsupportedMediaType(format!(
            "managed writes are not supported for mimetype {other} in this build \
             (supported: application/x-zarr, application/x-npy for array nodes; \
             application/x-parquet, text/csv for table nodes; \
             application/x-ragged+sql for ragged nodes; \
             application/x-awkward-buffers for awkward nodes; \
             application/x-parquet;structure=sparse for sparse nodes)"
        ))),
    }
}

/// Validate that a managed awkward-mimetype create carries an awkward structure
/// and return it. The awkward analog of [`managed_array_structure`]. The
/// structure is not needed to lay out storage (the buffer directory starts
/// empty), only to reject a create whose family/structure does not match.
fn managed_awkward_structure(
    ds: &crate::core::data_source::DataSource,
    mimetype: &str,
) -> Result<crate::core::structures::AwkwardStructure, ServerError> {
    if ds.structure_family != crate::core::structures::StructureFamily::Awkward {
        return Err(ServerError::UnsupportedMediaType(format!(
            "mimetype {mimetype} is only valid for awkward nodes, not {}",
            ds_family_str(ds.structure_family)
        )));
    }
    match &ds.structure {
        Some(crate::core::structures::AnyStructure::Awkward(a)) => Ok(a.clone()),
        _ => Err(ServerError::Validation(
            "a managed awkward create requires an awkward structure (form + length)".into(),
        )),
    }
}

/// Validate that a managed sparse-mimetype create carries a sparse structure and
/// return it. The sparse analog of [`managed_array_structure`]. The structure
/// (shape + chunks) drives the block layout `init_storage_sparse_parquet` lays
/// out, so a create whose family/structure does not match is rejected here.
///
/// `DataSource` deserialization now narrows `structure` under `structure_family`
/// authority ([`AnyStructure::from_family_json`](crate::core::structures::AnyStructure::from_family_json)),
/// so a sparse create yields `AnyStructure::Sparse` directly — this matches on
/// it like the array/table/ragged siblings. The previous re-derive-from-raw-JSON
/// workaround existed only because the untagged parse mislabeled a COO structure
/// (data_type + chunks + shape) as `Array`; that mislabel no longer happens.
#[cfg(feature = "parquet")]
fn managed_sparse_structure(
    ds: &crate::core::data_source::DataSource,
    mimetype: &str,
) -> Result<crate::core::structures::SparseStructure, ServerError> {
    if ds.structure_family != crate::core::structures::StructureFamily::Sparse {
        return Err(ServerError::UnsupportedMediaType(format!(
            "mimetype {mimetype} is only valid for sparse nodes, not {}",
            ds_family_str(ds.structure_family)
        )));
    }
    match &ds.structure {
        Some(crate::core::structures::AnyStructure::Sparse(s)) => Ok(s.clone()),
        _ => Err(ServerError::Validation(
            "a managed sparse create requires a sparse structure (shape + chunks)".into(),
        )),
    }
}

/// Validate that a managed ragged-mimetype create carries a ragged structure and
/// return it. The ragged analog of [`managed_array_structure`].
#[cfg(feature = "sql-adapter")]
fn managed_ragged_structure(
    ds: &crate::core::data_source::DataSource,
    mimetype: &str,
) -> Result<crate::core::structures::RaggedStructure, ServerError> {
    if ds.structure_family != crate::core::structures::StructureFamily::Ragged {
        return Err(ServerError::UnsupportedMediaType(format!(
            "mimetype {mimetype} is only valid for ragged nodes, not {}",
            ds_family_str(ds.structure_family)
        )));
    }
    match &ds.structure {
        Some(crate::core::structures::AnyStructure::Ragged(r)) => Ok(r.clone()),
        _ => Err(ServerError::Validation(
            "a managed ragged create requires a ragged structure (shape + chunks + size)".into(),
        )),
    }
}

/// Validate that a managed table-mimetype create carries a table structure and
/// return it. The table analog of [`managed_array_structure`].
#[cfg(any(feature = "csv-adapter", feature = "parquet-adapter"))]
fn managed_table_structure(
    ds: &crate::core::data_source::DataSource,
    mimetype: &str,
) -> Result<crate::core::structures::TableStructure, ServerError> {
    if ds.structure_family != crate::core::structures::StructureFamily::Table {
        return Err(ServerError::UnsupportedMediaType(format!(
            "mimetype {mimetype} is only valid for table nodes, not {}",
            ds_family_str(ds.structure_family)
        )));
    }
    match &ds.structure {
        Some(crate::core::structures::AnyStructure::Table(t)) => Ok(t.clone()),
        _ => Err(ServerError::Validation(
            "a managed table create requires a table structure (schema + columns)".into(),
        )),
    }
}

/// Validate that a managed array-mimetype create carries an array structure and
/// return it. Shared by every array `init_storage` arm so the family check and
/// the "needs shape + dtype" error stay identical across formats.
fn managed_array_structure(
    ds: &crate::core::data_source::DataSource,
    mimetype: &str,
) -> Result<crate::core::structures::ArrayStructure, ServerError> {
    if ds.structure_family != crate::core::structures::StructureFamily::Array {
        return Err(ServerError::UnsupportedMediaType(format!(
            "mimetype {mimetype} is only valid for array nodes, not {}",
            ds_family_str(ds.structure_family)
        )));
    }
    match &ds.structure {
        Some(crate::core::structures::AnyStructure::Array(a)) => Ok(a.clone()),
        _ => Err(ServerError::Validation(
            "a managed array create requires an array structure (shape + dtype)".into(),
        )),
    }
}

/// Map adapter-generated [`crate::core::data_source::Asset`]s to the catalog's
/// `AssetSpec` persistence shape. Shared by every `init_storage` arm.
fn to_asset_specs(
    assets: Vec<crate::core::data_source::Asset>,
) -> Vec<crate::catalog::data_source::AssetSpec> {
    assets
        .into_iter()
        .map(|a| crate::catalog::data_source::AssetSpec {
            data_uri: a.data_uri,
            is_directory: a.is_directory,
            parameter: a.parameter.unwrap_or_else(|| "data_uri".into()),
            num: a.num.map(|n| n as i32),
        })
        .collect()
}

fn map_catalog_err(e: crate::catalog::CatalogError) -> ServerError {
    use crate::catalog::CatalogError as CE;
    match e {
        CE::NotFound(m) => ServerError::NotFound(m),
        CE::Validation(m) => ServerError::Validation(m),
        CE::Conflict(m) => ServerError::Validation(m),
        // Deleting a subtree with internally-managed data sources → 409,
        // matching Python's WouldDeleteData handler (app.py:367-374).
        CE::WouldDeleteData(m) => ServerError::Conflict(m),
        // Database/Migration/Json/Io are all 500-class; the IntoResponse
        // impl logs the detail and returns a generic 500 body so we don't
        // leak DB internals to the client (R7).
        other => ServerError::Internal(other.to_string()),
    }
}

// ---------------------------------------------------------------------------
// PATCH /api/v1/metadata/{*path} — update metadata + specs
// ---------------------------------------------------------------------------
//
// The canonical client ALWAYS sends the HTTP header
// `Content-Type: application/json` and carries the real patch type plus the
// patch documents in the JSON body (tiled client/base.py:741-757):
//
//   `{ "content-type": <mimetype>, "metadata": <patch>, "specs": <patch>,
//      "access_blob": <patch> }`
//
// `metadata`, `specs` and `access_blob` are three INDEPENDENT patch
// documents — the discriminator lives in the body, not the transport
// header. Everything else (links, data_sources) is read-only here — use
// PUT /data_source for structural changes.

/// Body `content-type` discriminator: RFC 6902 ops array applied to each doc.
const JSON_PATCH_MIMETYPE: &str = "application/json-patch+json";
/// Body `content-type` discriminator: RFC 7396 partial doc merged into each.
const MERGE_PATCH_MIMETYPE: &str = "application/merge-patch+json";
/// Maximum specs a node may carry after a patch. Mirrors Python
/// `schemas.MAX_ALLOWED_SPECS` (= 20) and the catalog's private `MAX_SPECS`.
const MAX_ALLOWED_SPECS: usize = 20;

pub async fn patch_metadata(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    BaseUrl(base_url): BaseUrl,
    auth: crate::server::AuthContext,
    Json(req): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ServerError> {
    // Optional `?drop_revision=true` (upstream tiled #972). When set,
    // the previous (metadata, specs, access_blob) is discarded instead
    // of pushed onto the revisions table — useful for high-frequency
    // updates where the revision history would dominate storage.
    let drop_revision = params
        .get("drop_revision")
        .map(|v| matches!(v.as_str(), "true" | "True" | "1" | "yes"))
        .unwrap_or(false);
    let segments = segments_from_uri(&uri, "/api/v1/metadata/");
    let catalog = state.catalog.as_ref().ok_or_else(|| {
        ServerError::Validation("server has no catalog DB; PATCH not supported".into())
    })?;
    if segments.is_empty() {
        return Err(ServerError::Validation(
            "cannot PATCH the catalog root".into(),
        ));
    }
    // Per-ancestor auth gate: narrows at every prefix and requires
    // WriteMetadata on the narrowed set — same invariant as the read gate.
    // Capture the narrowed auth so its principal + scopes are available for
    // the modify_node call below.
    let auth = resolve_entry(&state, auth, &segments, crate::auth::Scope::WriteMetadata).await?;
    let node = catalog
        .lookup(&segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;

    // Patch dispatch (upstream tiled #688). The discriminator is the BODY
    // field `content-type` (it is data, not transport): the canonical client
    // always sends the HTTP header `Content-Type: application/json` and puts
    // the real patch type in the body (tiled client/base.py:741-757, server
    // router.py:2344-2368). `metadata`, `specs` and `access_blob` are three
    // INDEPENDENT patch documents, each applied to its own base:
    //
    //   * application/json-patch+json   — each field is an RFC 6902 ops array
    //     applied directly to that document;
    //   * application/merge-patch+json  — each field is a partial document
    //     merged per RFC 7396 (null deletes a key; an array replaces
    //     wholesale; a null/absent field means "no change");
    //   * anything else / missing       — 406 Not Acceptable. There is no
    //     silent fallback (mirrors Python's HTTP_406_NOT_ACCEPTABLE).
    let content_type = req
        .get("content-type")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let mode = if content_type == JSON_PATCH_MIMETYPE {
        PatchMode::JsonPatch
    } else if content_type == MERGE_PATCH_MIMETYPE {
        PatchMode::MergePatch
    } else {
        return Err(ServerError::NotAcceptable(format!(
            "valid content types: {JSON_PATCH_MIMETYPE}, {MERGE_PATCH_MIMETYPE}"
        )));
    };
    // Python treats `entry.specs or []`: a null/absent specs column is an
    // empty array for patch purposes.
    let base_specs = if node.specs.is_null() {
        serde_json::Value::Array(Vec::new())
    } else {
        node.specs.clone()
    };
    let (metadata, specs) = match mode {
        PatchMode::JsonPatch => {
            // RFC 6902 ops applied DIRECTLY to each document — `metadata` ops
            // target the metadata doc, `specs` ops target the specs array.
            // (No combined {metadata, specs} envelope: the ops paths are
            // relative to each document, matching Python's
            // `apply_json_patch(entry.metadata(), body.metadata or [])`.)
            let metadata = apply_json_patch_field(&node.metadata, req.get("metadata"))?;
            let specs = apply_json_patch_field(&base_specs, req.get("specs"))?;
            (metadata, specs)
        }
        PatchMode::MergePatch => {
            // RFC 7396 merge into each document. A null/absent field means
            // "no change"; for `specs`, an explicit array replaces wholesale.
            let mut metadata = node.metadata.clone();
            if let Some(patch) = req.get("metadata").filter(|v| !v.is_null()) {
                merge_patch_apply(&mut metadata, patch);
            }
            let specs = match req.get("specs") {
                None | Some(serde_json::Value::Null) => base_specs,
                Some(patch) => {
                    let mut specs = base_specs;
                    merge_patch_apply(&mut specs, patch);
                    specs
                }
            };
            (metadata, specs)
        }
    };

    // Limits that bypass register-time schema validation when reached via
    // patch — Python validates the FINAL specs in the handler before writing
    // (server router.py:2370-2380; both return HTTP 422). The catalog also
    // caps the count, but only the handler enforces uniqueness, and only here
    // do the messages mirror Python.
    if let Some(arr) = specs.as_array() {
        if arr.len() > MAX_ALLOWED_SPECS {
            return Err(ServerError::Validation(format!(
                "Update cannot result in more than {MAX_ALLOWED_SPECS} specs"
            )));
        }
        let mut seen: Vec<serde_json::Value> = Vec::with_capacity(arr.len());
        for spec in arr {
            let identity = spec_identity(spec);
            if seen.contains(&identity) {
                return Err(ServerError::Validation(
                    "Update cannot result in non-unique specs".into(),
                ));
            }
            seen.push(identity);
        }
    }

    // access_blob patch: apply the SAME json-patch / merge-patch step to the
    // stored access_blob that `metadata` and `specs` already get above (see the
    // `match mode` block), then hand the RESULT — not the raw patch document —
    // to policy.modify_node. Mirrors Python router.py:2351 (json-patch) /
    // :2364-2367 (merge-patch), with the policy call at :2397. A null/absent
    // access_blob field means "no change": no patched blob is produced and
    // modify_node is not consulted, so the stored blob is preserved. Errors
    // from the policy map to 422 (matching Python's ValueError path).
    let proposed_access_blob = match req.get("access_blob").filter(|v| !v.is_null()) {
        None => None,
        Some(patch_doc) => Some(match mode {
            PatchMode::JsonPatch => apply_json_patch_field(&node.access_blob, Some(patch_doc))?,
            PatchMode::MergePatch => {
                let mut blob = node.access_blob.clone();
                merge_patch_apply(&mut blob, patch_doc);
                blob
            }
        }),
    };
    let new_access_blob = if let (Some(policy), Some(principal), Some(proposed)) = (
        state.access_policy.as_deref(),
        auth.principal.as_deref(),
        proposed_access_blob.as_ref(),
    ) {
        let (modified, blob) = policy
            .modify_node(
                &node.access_blob,
                principal,
                None, // authn_access_tags: populated in sub-part 3
                &auth.scopes,
                Some(proposed),
            )
            .await
            .map_err(ServerError::Validation)?;
        if modified { Some(blob) } else { None }
    } else {
        None
    };

    let (updated, revision_number) = catalog
        .update_metadata(node.id, metadata, specs, new_access_blob, drop_revision)
        .await
        .map_err(map_catalog_err)?;
    let path = segments.join("/");
    // Publish `container-child-metadata-updated` on the *parent* node's stream
    // (upstream publishes on `self.node.parent`, adapter.py:1319-1334), keyed by
    // the child's key, carrying the new specs / metadata and — unless
    // `drop_revision` — the freshly-created revision number. A node with no
    // parent (root) has no subscribable parent id, so it does not stream.
    if let Some(parent_id) = node.parent_id {
        let seq = state.streaming_cache.incr_seq(parent_id).await;
        state
            .streaming_cache
            .set(
                parent_id,
                seq,
                crate::server::streaming_cache::StreamEvent::child_metadata_updated(
                    seq,
                    &updated.key,
                    updated.specs.clone(),
                    updated.metadata.clone(),
                    revision_number,
                ),
            )
            .await;
    }
    dispatch_webhook_event(
        &state,
        "metadata-updated",
        node.id,
        &path,
        metadata_updated_webhook_data(&updated.metadata, &updated.specs),
    )
    .await;
    let family = parse_structure_family(&updated.structure_family)?;
    let links = crate::core::links::links_for_node(family, &base_url, &path);
    Ok(Json(crate::core::schemas::PostMetadataResponse {
        id: updated.key,
        links: Some(serde_json::to_value(&links).unwrap_or_default()),
        metadata: Some(updated.metadata),
        data_sources: None,
        access_blob: Some(updated.access_blob),
    }))
}

// ---------------------------------------------------------------------------
// PUT /api/v1/metadata/{*path} — wholesale replace metadata / specs / access_blob
// ---------------------------------------------------------------------------
//
// Distinct from PATCH (a partial JSON-patch / merge-patch): PUT takes a full
// `{metadata, specs, access_blob}` document and replaces the stored values
// wholesale. Each field is optional; an absent / null field means "no change"
// (Python `PutMetadataRequest`, server/schemas.py:515-519). Mirrors Python
// `put_metadata` (server/router.py:2420-2494): resolve the entry, 405 if it
// has no `replace_metadata`, then `entry.replace_metadata(metadata, specs,
// access_blob, drop_revision)`.
pub async fn put_metadata(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    auth: crate::server::AuthContext,
    Json(req): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ServerError> {
    // Optional `?drop_revision=true` (upstream tiled #972): discard the prior
    // version instead of pushing it onto the revisions table.
    let drop_revision = params
        .get("drop_revision")
        .map(|v| matches!(v.as_str(), "true" | "True" | "1" | "yes"))
        .unwrap_or(false);
    let segments = segments_from_uri(&uri, "/api/v1/metadata/");
    // A node that cannot persist metadata (no catalog → in-memory tree) does
    // not support `replace_metadata` → 405, matching Python's "This node does
    // not support update of metadata." (router.py:2446-2450).
    let Some(catalog) = state.catalog.as_ref() else {
        return Err(ServerError::MethodNotAllowed(
            "This node does not support update of metadata.".into(),
        ));
    };
    if segments.is_empty() {
        return Err(ServerError::Validation(
            "cannot PUT the catalog root".into(),
        ));
    }
    // Per-ancestor auth gate: narrows at every prefix and requires
    // WriteMetadata on the narrowed set — same invariant as PATCH.
    let auth = resolve_entry(&state, auth, &segments, crate::auth::Scope::WriteMetadata).await?;
    let node = catalog
        .lookup(&segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;

    // Each field: a present, non-null value REPLACES the stored document;
    // absent / null means keep the current value (Python: `body.X if body.X is
    // not None else entry.X`). Python treats `entry.specs or []`: a null specs
    // column is an empty array.
    let metadata = match req.get("metadata") {
        Some(v) if !v.is_null() => v.clone(),
        _ => node.metadata.clone(),
    };
    let specs = match req.get("specs") {
        Some(v) if !v.is_null() => v.clone(),
        _ if node.specs.is_null() => serde_json::Value::Array(Vec::new()),
        _ => node.specs.clone(),
    };

    // Validate the FINAL specs (count ≤ 20, uniqueness) before writing — the
    // same limits the PATCH handler and Python `validate_specs` enforce (422).
    if let Some(arr) = specs.as_array() {
        if arr.len() > MAX_ALLOWED_SPECS {
            return Err(ServerError::Validation(format!(
                "Update cannot result in more than {MAX_ALLOWED_SPECS} specs"
            )));
        }
        let mut seen: Vec<serde_json::Value> = Vec::with_capacity(arr.len());
        for spec in arr {
            let identity = spec_identity(spec);
            if seen.contains(&identity) {
                return Err(ServerError::Validation(
                    "Update cannot result in non-unique specs".into(),
                ));
            }
            seen.push(identity);
        }
    }

    // access_blob: run the proposed blob through policy.modify_node.
    // Mirrors Python router.py:2484-2487: when modify_node signals a change
    // the new blob is written; otherwise the stored blob is preserved.
    let proposed_access_blob = req.get("access_blob").filter(|v| !v.is_null()).cloned();
    let new_access_blob = if let (Some(policy), Some(principal), Some(proposed)) = (
        state.access_policy.as_deref(),
        auth.principal.as_deref(),
        proposed_access_blob.as_ref(),
    ) {
        let (modified, blob) = policy
            .modify_node(
                &node.access_blob,
                principal,
                None, // authn_access_tags: populated in sub-part 3
                &auth.scopes,
                Some(proposed),
            )
            .await
            .map_err(ServerError::Validation)?;
        if modified { Some(blob) } else { None }
    } else {
        None
    };

    let (updated, revision_number) = catalog
        .update_metadata(
            node.id,
            metadata,
            specs,
            new_access_blob.clone(),
            drop_revision,
        )
        .await
        .map_err(map_catalog_err)?;

    // `access_blob_modified` is true when the stored blob changed (policy
    // modified it) or when the client sent a blob that differs from what is
    // stored (policy kept the original). The response echoes the final value
    // when it differs from what was stored before the call. Matches Python:
    // only emit access_blob in the response when it changed.
    let access_blob_modified = new_access_blob.is_some()
        || proposed_access_blob
            .as_ref()
            .map(|v| v != &updated.access_blob)
            .unwrap_or(false);

    let path = segments.join("/");
    // Publish `container-child-metadata-updated` on the *parent* node's stream
    // (upstream publishes on `self.node.parent`, adapter.py:1319-1334), keyed by
    // the child's key, with the new specs / metadata and — unless
    // `drop_revision` — the freshly-created revision number. A root node has no
    // subscribable parent id, so it does not stream.
    if let Some(parent_id) = node.parent_id {
        let seq = state.streaming_cache.incr_seq(parent_id).await;
        state
            .streaming_cache
            .set(
                parent_id,
                seq,
                crate::server::streaming_cache::StreamEvent::child_metadata_updated(
                    seq,
                    &updated.key,
                    updated.specs.clone(),
                    updated.metadata.clone(),
                    revision_number,
                ),
            )
            .await;
    }
    dispatch_webhook_event(
        &state,
        "metadata-updated",
        node.id,
        &path,
        metadata_updated_webhook_data(&updated.metadata, &updated.specs),
    )
    .await;

    // Response mirrors Python `json_or_msgpack(response_data)`: `{id}` plus
    // `access_blob` only when modified.
    Ok(Json(crate::core::schemas::PostMetadataResponse {
        id: updated.key,
        links: None,
        metadata: None,
        data_sources: None,
        access_blob: if access_blob_modified {
            Some(updated.access_blob)
        } else {
            None
        },
    }))
}

// ---------------------------------------------------------------------------
// GET /api/v1/revisions/{*path} — list a node's metadata revision history.
// DELETE /api/v1/revisions/{*path}?number=N — drop one revision.
// Python parity: router.py:2496-2535 (get_revisions / delete_revision).
// Revisions are a catalog capability: a server without a catalog has no node
// that supports them → 405.
// ---------------------------------------------------------------------------

pub async fn get_revisions(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    BaseUrl(base_url): BaseUrl,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::ReadMetadata)?;
    let segments = segments_from_uri(&uri, "/api/v1/revisions/");
    let offset: usize = params
        .get("page[offset]")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let limit: usize = params
        .get("page[limit]")
        .and_then(|v| v.parse().ok())
        .unwrap_or(links::DEFAULT_PAGE_SIZE)
        .min(links::MAX_PAGE_SIZE);

    // A node that does not persist revisions (no catalog → in-memory tree)
    // does not support them → 405, matching Python's "This node does not
    // support revisions." (router.py:2521-2525).
    let Some(catalog) = state.catalog.as_ref() else {
        return Err(ServerError::MethodNotAllowed(
            "This node does not support revisions.".into(),
        ));
    };
    if segments.is_empty() {
        return Err(ServerError::Validation(
            "the catalog root has no revisions".into(),
        ));
    }
    // Per-ancestor auth gate (read:metadata), identical to the metadata read.
    resolve_entry(&state, auth, &segments, crate::auth::Scope::ReadMetadata).await?;
    let node = catalog
        .lookup(&segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;

    let revisions = catalog
        .list_revisions(node.id, offset, limit)
        .await
        .map_err(map_catalog_err)?;
    // `meta.count` and the `next`/`last` pagination links must reflect the
    // TOTAL revision count for the node, not this page's length. Feeding the
    // page length made `pagination_links` derive `last_offset = 0` and never
    // emit a `next` link, so clients could not page past the first page
    // (upstream #1409, closes #1389). The page still comes from
    // `list_revisions`; `count_revisions` supplies the page-independent total.
    let count = catalog
        .count_revisions(node.id)
        .await
        .map_err(map_catalog_err)? as usize;
    // Each item: `{revision_number, attributes: {metadata, specs, time_updated}}`
    // (Python construct_revisions_response, server/core.py:339-348). access_blob
    // is intentionally not surfaced.
    let data: Vec<serde_json::Value> = revisions
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "revision_number": r.revision_number,
                "attributes": {
                    "metadata": r.metadata,
                    "specs": r.specs,
                    "time_updated": r.time_updated,
                },
            })
        })
        .collect();
    let path = segments.join("/");
    let pg_links = links::pagination_links(
        &base_url,
        "revisions",
        &path,
        None,
        offset,
        limit,
        None,
        count,
    );
    Ok(Json(serde_json::json!({
        "data": data,
        "links": pg_links,
        "meta": {"count": count},
    })))
}

pub async fn delete_revision(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(crate::auth::Scope::DeleteRevision)?;
    let segments = segments_from_uri(&uri, "/api/v1/revisions/");
    // `?number=N` is required (Python `number: int` is a mandatory query param).
    let number: i64 = params
        .get("number")
        .ok_or_else(|| ServerError::Validation("query parameter 'number' is required".into()))?
        .parse()
        .map_err(|_| ServerError::Validation("'number' must be an integer".into()))?;

    let Some(catalog) = state.catalog.as_ref() else {
        return Err(ServerError::MethodNotAllowed(
            "This node does not support a del request for revisions.".into(),
        ));
    };
    if segments.is_empty() {
        return Err(ServerError::Validation(
            "the catalog root has no revisions".into(),
        ));
    }
    // Per-ancestor auth gate; the terminal node additionally needs
    // delete:revision (Python get_entry scopes=["delete:revision"]).
    resolve_entry(&state, auth, &segments, crate::auth::Scope::DeleteRevision).await?;
    let node = catalog
        .lookup(&segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;

    let deleted = catalog
        .delete_revision(node.id, number)
        .await
        .map_err(map_catalog_err)?;
    if !deleted {
        // Python raises 404 when rowcount == 0 (catalog/adapter.py:1207-1212).
        return Err(ServerError::NotFound(format!(
            "No revision {number} for node '{}'",
            segments.join("/")
        )));
    }
    // Python returns json_or_msgpack(None) → a `null` body with 200.
    Ok(Json(serde_json::Value::Null))
}

// ---------------------------------------------------------------------------
// GET /api/v1/asset/bytes/{*path} + /api/v1/asset/manifest/{*path}
// ---------------------------------------------------------------------------
//
// Raw-asset download (upstream tiled router.py:2570-2723). Gated by
// `expose_raw_assets` (default true). Only `file://` assets are served. A
// directory asset requires a `relative_path` (one of the entries from
// /asset/manifest); a single-file asset must not have one.

/// Required `id` query param → the asset id (Python `id: int`).
fn parse_asset_id(params: &HashMap<String, String>) -> Result<i64, ServerError> {
    params
        .get("id")
        .ok_or_else(|| ServerError::Validation("query parameter 'id' is required".into()))?
        .parse::<i64>()
        .map_err(|_| ServerError::Validation("'id' must be an integer".into()))
}

/// Convert an asset `file://` data_uri to a filesystem path (Python
/// `path_from_uri`, utils.py:745) via the shared cross-platform
/// [`crate::core::file_uri`] parser, which handles the `file://host/path` and
/// `file:///path` forms (and `file:///C:/...` on Windows) plus percent-decoding.
fn path_from_file_uri(uri: &str) -> Result<std::path::PathBuf, ServerError> {
    // The `file:` prefix is validated by the caller before this runs, so a
    // failure here means the stored data_uri is corrupt — a server-side data
    // integrity problem, not bad client input. Classify as Internal (500) so
    // the URI is logged server-side and not echoed to the client.
    crate::core::file_uri::file_uri_to_path(uri)
        .ok_or_else(|| ServerError::Internal(format!("invalid asset data_uri '{uri}'")))
}

/// Shared prelude for both asset endpoints, in Python's check order
/// (router.py:2584-2615): resolve + scope-gate the entry, apply the
/// `expose_raw_assets` policy (403), require a catalog (== Python's
/// `hasattr(entry, "asset_by_id")` → 405 for the in-memory tree), look up the
/// node, then fetch the node-scoped asset (404 if absent).
async fn resolve_asset(
    state: &AppState,
    auth: crate::server::AuthContext,
    segments: &[String],
    asset_id: i64,
) -> Result<crate::catalog::orm::Asset, ServerError> {
    // Per-ancestor auth gate + terminal read:data (Python get_entry
    // scopes=["read:data"]). resolve_entry narrows per node and enforces
    // read:data at the resolved entry. Bad path → 404, insufficient scope → 403.
    resolve_entry(state, auth, segments, crate::auth::Scope::ReadData).await?;
    // expose_raw_assets policy (router.py:2596-2603).
    if !state.expose_raw_assets {
        return Err(ServerError::Forbidden(
            "This Tiled server is configured not to allow downloading raw assets.".into(),
        ));
    }
    // No catalog → the in-memory tree adapters have no `asset_by_id`, mirroring
    // Python's `hasattr` check (router.py:2604-2608) → 405.
    let Some(catalog) = state.catalog.as_ref() else {
        return Err(ServerError::MethodNotAllowed(
            "This node does not support downloading assets.".into(),
        ));
    };
    let node = catalog
        .lookup(segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;
    catalog
        .asset_by_id(node.id, asset_id)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| {
            ServerError::NotFound(format!(
                "This node exists but it does not have an Asset with id {asset_id}"
            ))
        })
}

pub async fn get_asset_bytes(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    let segments = segments_from_uri(&uri, "/api/v1/asset/bytes/");
    let asset_id = parse_asset_id(&params)?;
    let asset = resolve_asset(&state, auth, &segments, asset_id).await?;

    let relative_path = params.get("relative_path").filter(|s| !s.is_empty());
    // Directory vs single-file relative_path rules (router.py:2616-2635).
    if asset.is_directory {
        let Some(rel) = relative_path else {
            return Err(ServerError::BadRequest(format!(
                "This asset is a directory. Must specify relative path, from \
                 manifest provided by /asset/manifest/...?id={asset_id}"
            )));
        };
        // Reject an absolute path under *either* OS convention. std::Path's
        // is_absolute() is platform-specific — on Windows a POSIX "/etc/passwd"
        // is not "absolute", so it would slip past this guard and `join` below
        // would resolve it to a real on-disk path (404 instead of the intended
        // 400). A leading '/' or '\\' covers the POSIX/UNC roots; a Windows
        // drive-letter path ("C:\\...") is caught by is_absolute on Windows.
        if std::path::Path::new(rel).is_absolute() || rel.starts_with('/') || rel.starts_with('\\')
        {
            return Err(ServerError::BadRequest(
                "relative_path query parameter must be a *relative* path".into(),
            ));
        }
    } else if relative_path.is_some() {
        return Err(ServerError::BadRequest(
            "This asset is not a directory. The relative_path query parameter must not be set."
                .into(),
        ));
    }

    if !asset.data_uri.starts_with("file:") {
        return Err(ServerError::BadRequest(
            "Only download assets stored as file:// is currently supported.".into(),
        ));
    }
    let base = path_from_file_uri(&asset.data_uri)?;
    let full_path = match relative_path {
        Some(rel) => {
            // Correct traversal guard. (Python's check at router.py:2645,
            // `not commonpath([p, p/rel]) != p`, is inverted and would reject
            // the valid case; we implement the documented intent: refuse to
            // serve anything that canonicalizes outside the asset directory,
            // e.g. a `../..` escape that survived the not-absolute check.)
            let canonical_base = tokio::fs::canonicalize(&base)
                .await
                .map_err(|e| map_asset_io_err(e, "asset directory"))?;
            let candidate = canonical_base.join(rel);
            let resolved = tokio::fs::canonicalize(&candidate)
                .await
                .map_err(|e| map_asset_io_err(e, "requested file"))?;
            if !resolved.starts_with(&canonical_base) {
                return Err(ServerError::BadRequest(
                    "relative_path escapes the asset directory".into(),
                ));
            }
            resolved
        }
        None => base,
    };

    let filename = full_path
        .file_name()
        .and_then(|n| n.to_str())
        .map(str::to_string)
        .unwrap_or_else(|| "download".to_string());
    let bytes = tokio::fs::read(&full_path)
        .await
        .map_err(|e| map_asset_io_err(e, "asset file"))?;

    // Raw bytes: octet-stream + attachment so any client downloads rather than
    // renders. serve_with_range adds Range support (resumable large downloads).
    let mut resp = serve_with_range(
        &headers,
        "application/octet-stream",
        bytes::Bytes::from(bytes),
        None,
    );
    if let Ok(value) = axum::http::HeaderValue::from_str(&format!(
        "attachment; filename=\"{}\"",
        filename.replace('"', "")
    )) {
        resp.headers_mut()
            .insert(axum::http::header::CONTENT_DISPOSITION, value);
    }
    Ok(resp)
}

pub async fn get_asset_manifest(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    let segments = segments_from_uri(&uri, "/api/v1/asset/manifest/");
    let asset_id = parse_asset_id(&params)?;
    let asset = resolve_asset(&state, auth, &segments, asset_id).await?;

    if !asset.is_directory {
        return Err(ServerError::BadRequest(
            "This asset is not a directory. There is no manifest.".into(),
        ));
    }
    if !asset.data_uri.starts_with("file:") {
        return Err(ServerError::BadRequest(
            "Only download assets stored as file:// is currently supported.".into(),
        ));
    }
    let dir = path_from_file_uri(&asset.data_uri)?;
    // Manifest entries are paths RELATIVE to the asset directory. Python
    // documents the manifest as relative (client/base.py:342) and the client
    // feeds each entry straight back as `relative_path`; the Python *server*
    // emits absolute paths via `Path(root, file)` (router.py:2722), which the
    // /asset/bytes endpoint then rejects as absolute — a round-trip bug. Rust
    // returns relative paths so manifest → relative_path → bytes works.
    let manifest = tokio::task::spawn_blocking(move || collect_manifest(&dir))
        .await
        .map_err(|e| ServerError::Internal(format!("manifest walk task failed: {e}")))??;
    Ok(Json(serde_json::json!({ "manifest": manifest })))
}

/// Map a filesystem error from asset serving: a missing file/dir is a 404
/// (the asset row exists but its backing file is gone), anything else a 500.
fn map_asset_io_err(e: std::io::Error, what: &str) -> ServerError {
    if e.kind() == std::io::ErrorKind::NotFound {
        ServerError::NotFound(format!("{what} not found on disk"))
    } else {
        ServerError::Internal(format!("reading {what}: {e}"))
    }
}

/// Recursively collect every regular file under `dir`, returned as
/// forward-slash paths relative to `dir`, sorted for deterministic output.
/// Symlinked directories are not descended (matching `os.walk`'s default
/// `followlinks=False`); a symlink is recorded as a leaf entry.
fn collect_manifest(dir: &std::path::Path) -> Result<Vec<String>, ServerError> {
    fn walk(
        base: &std::path::Path,
        cur: &std::path::Path,
        out: &mut Vec<String>,
    ) -> Result<(), ServerError> {
        let entries = std::fs::read_dir(cur).map_err(|e| map_asset_io_err(e, "asset directory"))?;
        for entry in entries {
            let entry = entry.map_err(|e| map_asset_io_err(e, "asset directory entry"))?;
            let file_type = entry
                .file_type()
                .map_err(|e| map_asset_io_err(e, "asset directory entry"))?;
            let path = entry.path();
            if file_type.is_dir() {
                walk(base, &path, out)?;
            } else {
                let rel = path.strip_prefix(base).unwrap_or(&path);
                out.push(rel.to_string_lossy().replace('\\', "/"));
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out)?;
    out.sort();
    Ok(out)
}

// ---------------------------------------------------------------------------
// PUT /api/v1/data_source/{*path} — replace structure / parameters
// ---------------------------------------------------------------------------
//
// JSON body: `{ "data_source": { id, structure, parameters } }`. Asset
// rewrite is intentionally out of scope here — adding/removing assets goes
// through register so the FK + transaction guarantees stay simple.

pub async fn put_data_source(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    auth: crate::server::AuthContext,
    Json(req): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ServerError> {
    let segments = segments_from_uri(&uri, "/api/v1/data_source/");
    let catalog = state.catalog.as_ref().ok_or_else(|| {
        ServerError::Validation("server has no catalog DB; PUT not supported".into())
    })?;
    if segments.is_empty() {
        return Err(ServerError::Validation(
            "PUT /data_source requires a node path".into(),
        ));
    }
    // Rewriting a node's storage mapping requires BOTH write:metadata AND
    // register, matching upstream (router.py:1944/1948:
    // `Security(check_scopes, ["write:metadata","register"])` +
    // `get_entry(path, ["write:metadata","register"])`). register is what keeps
    // a plain `user` — who holds write:data/write:metadata but not register —
    // from repointing a node at different storage.
    let auth = resolve_entry_catalog(&state, auth, &segments).await?;
    auth.require(crate::auth::Scope::WriteMetadata)?;
    auth.require(crate::auth::Scope::Register)?;
    let node = catalog
        .lookup(&segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;
    // `?patch_shape=`/`?patch_offset=` are comma-separated index tuples that go
    // together: both present -> an `ArrayPatch`-shaped `{shape, offset}` patch
    // descriptor; both absent -> no patch; exactly one -> 400. Upstream checks
    // this right after resolving the node and before the data-source
    // existence/ownership check (router.py:1946-1973 -> adapter.py rowcount), so
    // a one-sided patch 400s regardless of the target id.
    let patch_shape = params.get("patch_shape").filter(|s| !s.is_empty());
    let patch_offset = params.get("patch_offset").filter(|s| !s.is_empty());
    let patch: Option<serde_json::Value> = match (patch_shape, patch_offset) {
        (None, None) => None,
        (Some(sh), Some(off)) => Some(serde_json::json!({
            "shape": parse_csv_usize(sh)?,
            "offset": parse_csv_usize(off)?,
        })),
        _ => {
            // Upstream concatenates two adjacent string literals with no
            // separating space ("patch_offset" + "go together"), so the wire
            // detail reads "patch_offsetgo together"; reproduced verbatim
            // (router.py:1969-1972).
            return Err(ServerError::BadRequest(
                "The query parameters patch_shape and patch_offsetgo together; \
                 either all or none must be specified."
                    .into(),
            ));
        }
    };
    let body = req
        .get("data_source")
        .ok_or_else(|| ServerError::Validation("body missing 'data_source'".into()))?;
    let id = body
        .get("id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| ServerError::Validation("'data_source.id' missing".into()))?;
    // Sanity: the targeted data_source must belong to the resolved node.
    let owned = catalog
        .list_data_sources(node.id)
        .await
        .map_err(map_catalog_err)?;
    if !owned.iter().any(|d| d.id == id) {
        return Err(ServerError::NotFound(format!(
            "data_source {id} does not belong to '{}'",
            segments.join("/")
        )));
    }

    let structure = body.get("structure").cloned().unwrap_or_default();
    let parameters = body.get("parameters").cloned().unwrap_or_default();
    // Snapshot the array shape and the request data-source object before
    // `structure`/`body` are consumed, so the post-write `array-ref` emit can
    // reuse them (upstream metadata uses the request `data_source`/`structure`).
    let array_shape: Option<Vec<usize>> = structure
        .get("shape")
        .and_then(serde_json::Value::as_array)
        .and_then(|arr| {
            arr.iter()
                .map(|v| v.as_u64().map(|n| n as usize))
                .collect::<Option<Vec<usize>>>()
        });
    let data_source_json = body.clone();
    let updated = catalog
        .update_data_source(id, structure, parameters)
        .await
        .map_err(map_catalog_err)?;

    // Live-streaming: upstream emits an `array-ref` event AFTER the commit, but
    // ONLY for array-family data sources — the event carries a shape so a
    // subscriber can build a slice URI (catalog/adapter.py:973-992). Metadata
    // only, no payload. Best-effort; the cache is a no-op in non-streaming builds.
    if updated.structure_family == "array"
        && let Some(shape) = array_shape.as_deref()
    {
        stream_array_ref(&state, node.id, data_source_json, patch, shape).await;
    }

    Ok(Json(serde_json::json!({"data_source": {
        "id": updated.id,
        "structure_family": updated.structure_family,
        "structure": updated.structure,
        "mimetype": updated.mimetype,
        "parameters": updated.parameters,
        "management": updated.management,
    }})))
}

// ---------------------------------------------------------------------------
// DELETE /api/v1/metadata/{*path} — remove a node (cascade)
// ---------------------------------------------------------------------------

pub async fn delete_metadata(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    let segments = segments_from_uri(&uri, "/api/v1/metadata/");
    // Safety gate, on by default (Python `external_only=True`, router.py:1979).
    // When true, the catalog refuses to delete a subtree that holds any
    // internally-managed data source (would orphan the storage files).
    // Only an explicit false-ish value disables it; an unrecognized value
    // keeps the gate on, the data-safe choice.
    let external_only = params
        .get("external_only")
        .map(|v| {
            !matches!(
                v.to_ascii_lowercase().as_str(),
                "false" | "0" | "no" | "off"
            )
        })
        .unwrap_or(true);
    // `?recursive=true` deletes the whole subtree in one call; default false,
    // matching Python `recursive: bool = Query(False)` (router.py:1980). When
    // false, a non-empty container is refused (the empty-check below); when
    // true the check is skipped and the catalog's `delete_node` cascades the
    // subtree. `external_only` still governs managed data independently.
    let recursive = params
        .get("recursive")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
        .unwrap_or(false);
    let catalog = state.catalog.as_ref().ok_or_else(|| {
        ServerError::Validation("server has no catalog DB; DELETE not supported".into())
    })?;
    if segments.is_empty() {
        return Err(ServerError::Validation(
            "cannot DELETE the catalog root".into(),
        ));
    }
    // Per-ancestor auth gate: narrow at every prefix, require DeleteNode on the
    // fully-narrowed set, then DeleteRevision on that same context. Deleting a
    // node cascade-destroys its revision history, so upstream gates this route
    // with BOTH delete:node AND delete:revision (router.py:1995 global Security +
    // :1999 get_entry, both scopes, both layers). The built-in roles bundle the
    // two (for_role), so this only tightens a custom AccessPolicy that grants
    // delete:node without delete:revision.
    let auth = resolve_entry(&state, auth, &segments, crate::auth::Scope::DeleteNode).await?;
    auth.require(crate::auth::Scope::DeleteRevision)?;
    let node = catalog
        .lookup(&segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;
    // Reject deletion of a non-empty container unless `recursive=true`
    // (upstream tiled #503; `if not recursive` gate at adapter.py:1069-1085).
    // Cascading FK delete *would* succeed, but silently dropping a subtree is
    // the kind of thing that needs explicit `rm -rf` semantics; a
    // non-recursive caller must empty the container first. With recursive=true
    // the caller has opted in and `delete_node` cascades the whole subtree.
    if !recursive && node.structure_family == "container" {
        let kid_count = catalog
            .count_children(Some(node.id))
            .await
            .map_err(map_catalog_err)?;
        if kid_count > 0 {
            // 409 Conflict, matching Python's Conflicts handler
            // (adapter.py:1024 -> app.py:350-353).
            return Err(ServerError::Conflict(format!(
                "container '{}' is not empty ({kid_count} children); \
                 delete its contents first or pass recursive=true",
                segments.join("/"),
            )));
        }
    }
    catalog
        .delete_node(node.id, external_only)
        .await
        .map_err(map_catalog_err)?;
    let path = segments.join("/");
    // Publish `node-deleted` on the deleted node's own stream (tiled-rs
    // extension, D9 — upstream's `delete()` emits no streaming event), then close
    // the stream. A live subscriber on this node receives the `node-deleted`
    // event and is then disconnected by the following `end_of_stream` (WS close
    // 1000, "Producer ended stream"), rather than hanging on a node that can
    // never produce again.
    let seq = state.streaming_cache.incr_seq(node.id).await;
    state
        .streaming_cache
        .set(
            node.id,
            seq,
            crate::server::streaming_cache::StreamEvent::node_deleted(seq),
        )
        .await;
    // `close` does its own incr_seq + set(end_of_stream); ordering set(node-deleted)
    // THEN close guarantees the deletion notice reaches the subscriber first.
    state.streaming_cache.close(node.id).await;
    dispatch_webhook_event(
        &state,
        "node-deleted",
        node.id,
        &path,
        serde_json::json!({ "type": "node-deleted" }),
    )
    .await;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

/// `DELETE /api/v1/stream/close/{path}` — a producer ends a node's stream.
///
/// Upstream `close_stream` (router.py:725-748) is gated by `write:data`
/// (`Security(check_scopes, ["write:data"])` + `get_entry(path, ["write:data"])`)
/// and then calls `entry.close_stream()`, which (adapter.py:1365-1380):
///   1. `streaming_cache.close(node.id)` — emits an `end_of_stream` marker so
///      every live subscriber disconnects (the WS consumer closes with 1000
///      "Producer ended stream"); the disabled cache is a no-op; and
///   2. fires a `stream-closed` webhook on the node's *own* id (so webhooks
///      registered directly on this node are included in the ancestor walk).
///
/// `StreamClosedEvent` (schemas.py:657-661) carries only the node key —
/// path/timestamp/event_type are the dispatcher's common envelope fields.
/// Returns 200 with no body (upstream returns `None`).
pub async fn close_stream(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    auth: crate::server::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    let segments = segments_from_uri(&uri, "/api/v1/stream/close/");
    let catalog = state.catalog.as_ref().ok_or_else(|| {
        ServerError::Validation("server has no catalog DB; stream close not supported".into())
    })?;
    if segments.is_empty() {
        return Err(ServerError::Validation(
            "DELETE /stream/close requires a node path".into(),
        ));
    }
    // Ending a stream requires `write:data` on the node, matching upstream's
    // `Security(check_scopes, ["write:data"])` + `get_entry(path, ["write:data"])`
    // (router.py:734/735-747). `resolve_entry` 404s a missing/invisible node and
    // 403s a caller lacking `write:data` on the narrowed node.
    resolve_entry(&state, auth, &segments, crate::auth::Scope::WriteData).await?;
    let node = catalog
        .lookup(&segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;
    // (1) End the stream: `close` emits an `end_of_stream` marker that the WS
    // consumer turns into a clean close (1000 "Producer ended stream"),
    // disconnecting live subscribers (adapter.py:1366-1367).
    state.streaming_cache.close(node.id).await;
    // (2) Fire the `stream-closed` webhook on the node's own id (adapter.py:
    // 1368-1380). The body carries only the node key; the dispatcher stamps the
    // common `event_type`/`path`/`sequence`/`timestamp` envelope fields.
    dispatch_webhook_event(
        &state,
        "stream-closed",
        node.id,
        &segments.join("/"),
        serde_json::json!({ "type": "stream-closed", "key": node.key }),
    )
    .await;
    Ok(axum::http::StatusCode::OK)
}

/// Build a `Resource` for the catalog by reading the DB directly. Skips
/// the `CatalogAdapter`'s in-memory cache so a same-request read after a
/// write sees the latest state.
async fn catalog_metadata_resource(
    catalog: &crate::catalog::Catalog,
    root_tree: &dyn ContainerAdapter,
    segments: &[String],
    base_url: &str,
    include_data_sources: bool,
    exact_count_limit: i64,
) -> Result<crate::core::schemas::Resource, ServerError> {
    use crate::core::schemas::{
        NodeAttributes, NodeStructure, Resource, SortDirection, SortingItem,
    };
    if segments.is_empty() {
        // Root container length: exact for small containers, statistics-based
        // approximation for large ones on Postgres (SQLite stays exact).
        let count = catalog
            .count_children_or_approx(None, exact_count_limit)
            .await
            .map_err(map_catalog_err)?;
        let links = crate::core::links::links_for_node(
            crate::core::structures::StructureFamily::Container,
            base_url,
            "",
        );
        return Ok(Resource {
            id: String::new(),
            attributes: NodeAttributes {
                ancestors: vec![],
                structure_family: Some(crate::core::structures::StructureFamily::Container),
                specs: Some(vec![]),
                metadata: Some(serde_json::Value::Object(Default::default())),
                structure: Some(
                    serde_json::to_value(&NodeStructure {
                        contents: None,
                        count: count as usize,
                    })
                    .unwrap_or_default(),
                ),
                access_blob: None,
                sorting: Some(vec![SortingItem {
                    key: "_".into(),
                    direction: SortDirection::Ascending,
                }]),
                data_sources: None,
            },
            links,
        });
    }

    let node = match catalog.lookup(segments).await.map_err(map_catalog_err)? {
        Some(node) => node,
        None => {
            // No DB node for this path. A `[table, column]` path addresses a
            // synthesized array child of a table leaf, which the catalog does
            // not index — upstream `lookup_adapter` falls back to
            // `adapter.get(segment)` on the deepest data-source-backed node
            // (catalog/adapter.py:557-566). When the parent is a table, resolve
            // the column through the same path the array/zarr routes use —
            // `walk_tree` synthesizes it via `core::table_column_as_array` — and
            // build the resource from that array adapter with the shared
            // `construct_resource`, so every route agrees on a catalog-backed
            // server. A column absent from the schema 404s inside `walk_tree`.
            if segments.len() >= 2 {
                let parent = &segments[..segments.len() - 1];
                let parent_is_table = catalog
                    .lookup(parent)
                    .await
                    .map_err(map_catalog_err)?
                    .map(|n| n.structure_family == "table")
                    .unwrap_or(false);
                if parent_is_table {
                    let adapter = core::walk_tree(root_tree, segments).await?;
                    let id = segments.last().cloned().unwrap_or_default();
                    let path = segments.join("/");
                    return core::construct_resource(&adapter, &id, &path, base_url).await;
                }
            }
            return Err(ServerError::NotFound(format!(
                "'{}' not found",
                segments.join("/")
            )));
        }
    };
    let path = segments.join("/");
    let id = segments.last().cloned().unwrap_or_default();
    let family = parse_structure_family(&node.structure_family)?;
    let links = crate::core::links::links_for_node(family, base_url, &path);
    let ancestors = if segments.len() > 1 {
        segments[..segments.len() - 1].to_vec()
    } else {
        vec![]
    };
    // For container nodes, surface the child count. Leaves carry their
    // data-source structure; when include_data_sources is set, also return
    // the full data_sources list (with assets) so clients can inspect asset
    // URIs, mimetypes, and management info without a separate request.
    let (structure_value, data_sources) =
        if matches!(family, crate::core::structures::StructureFamily::Container) {
            // Container length: exact for small containers, statistics-based
            // approximation for large ones on Postgres (SQLite stays exact).
            let count = catalog
                .count_children_or_approx(Some(node.id), exact_count_limit)
                .await
                .map_err(map_catalog_err)?;
            (
                Some(
                    serde_json::to_value(&NodeStructure {
                        contents: None,
                        count: count as usize,
                    })
                    .unwrap_or_default(),
                ),
                None,
            )
        } else {
            let ds_rows = catalog
                .list_data_sources(node.id)
                .await
                .map_err(map_catalog_err)?;
            let sv = ds_rows.first().map(|ds| ds.structure.clone());
            let ds_list = if include_data_sources {
                let mut result = Vec::with_capacity(ds_rows.len());
                for ds in ds_rows {
                    let asset_rows = catalog.list_assets(ds.id).await.map_err(map_catalog_err)?;
                    result.push(crate::catalog::data_source::to_core_data_source(
                        ds, asset_rows,
                    ));
                }
                Some(result)
            } else {
                None
            };
            (sv, ds_list)
        };
    let sorting = if matches!(family, crate::core::structures::StructureFamily::Container) {
        Some(vec![SortingItem {
            key: "_".into(),
            direction: SortDirection::Ascending,
        }])
    } else {
        None
    };
    Ok(Resource {
        id,
        attributes: NodeAttributes {
            ancestors,
            structure_family: Some(family),
            specs: serde_json::from_value(node.specs).unwrap_or_default(),
            metadata: Some(node.metadata),
            structure: structure_value,
            access_blob: Some(node.access_blob),
            sorting,
            data_sources,
        },
        links,
    })
}

/// Map a DB `structure_family` string to the enum. Delegates to the canonical
/// [`StructureFamily`](crate::core::structures::StructureFamily) `FromStr` so it
/// stays in lockstep with every family the core knows — this used to be a local
/// `match` that drifted and omitted `ragged`, 422-ing every ragged node read
/// once ragged became catalog-creatable.
fn parse_structure_family(
    s: &str,
) -> Result<crate::core::structures::StructureFamily, ServerError> {
    s.parse().map_err(|e: crate::core::TiledError| {
        ServerError::Validation(format!("unknown structure_family in DB: {s} ({e})"))
    })
}

#[cfg(test)]
mod sort_param_tests {
    use super::*;

    fn p(pairs: &[(&str, &str)]) -> Vec<(String, SortDirection)> {
        let owned: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        parse_sort(&owned)
    }

    #[test]
    fn empty_param_yields_no_sorting() {
        // Old clients send a bare `sort=`; it must not produce a sort key.
        assert!(p(&[("sort", "")]).is_empty());
        assert!(p(&[]).is_empty());
    }

    #[test]
    fn comma_separated_keys_with_directions() {
        let got = p(&[("sort", "color,-count,+name")]);
        assert_eq!(
            got,
            vec![
                ("color".to_string(), SortDirection::Ascending),
                ("count".to_string(), SortDirection::Descending),
                ("name".to_string(), SortDirection::Ascending),
            ]
        );
    }

    #[test]
    fn leading_minus_is_descending() {
        assert_eq!(
            p(&[("sort", "-time")]),
            vec![("time".to_string(), SortDirection::Descending)]
        );
    }

    #[test]
    fn bare_minus_is_default_direction_sentinel() {
        // A bare "-" → empty key, descending: the default-direction sentinel.
        assert_eq!(
            p(&[("sort", "-")]),
            vec![(String::new(), SortDirection::Descending)]
        );
    }

    #[test]
    fn repeated_sort_params_accumulate() {
        assert_eq!(
            p(&[("sort", "a"), ("sort", "-b")]),
            vec![
                ("a".to_string(), SortDirection::Ascending),
                ("b".to_string(), SortDirection::Descending),
            ]
        );
    }

    #[test]
    fn non_sort_params_ignored() {
        assert!(p(&[("page[limit]", "10"), ("filter[eq]", "x")]).is_empty());
    }
}

#[cfg(test)]
mod block_parser_tests {
    use super::*;

    #[test]
    fn parse_single_int() {
        let s = BlockSpec::parse("3").unwrap();
        assert!(matches!(s, BlockSpec::Single(3)));
        assert_eq!(s.range(), (3, 4));
    }

    #[test]
    fn parse_range() {
        let s = BlockSpec::parse("2:5").unwrap();
        assert!(matches!(s, BlockSpec::Range(2, 5)));
        assert_eq!(s.range(), (2, 5));
    }

    #[test]
    fn parse_invalid_int_rejected() {
        assert!(BlockSpec::parse("abc").is_err());
    }

    #[test]
    fn parse_range_stop_le_start_rejected() {
        assert!(BlockSpec::parse("3:3").is_err());
        assert!(BlockSpec::parse("4:2").is_err());
    }

    #[test]
    fn copy_chunk_1d() {
        let mut result = vec![0u8; 10];
        let chunk = vec![1u8, 2, 3, 4];
        copy_chunk_into_result(&mut result, &[10], &[3], &chunk, &[4], 1);
        assert_eq!(result, vec![0, 0, 0, 1, 2, 3, 4, 0, 0, 0]);
    }

    #[test]
    fn copy_chunk_2d() {
        // result is 4x4, chunk is 2x2, place at offset (1, 1):
        //  . . . .       . . . .
        //  . a b .   →   . a b .
        //  . c d .       . c d .
        //  . . . .       . . . .
        let mut result = vec![0u8; 16];
        let chunk = vec![b'a', b'b', b'c', b'd'];
        copy_chunk_into_result(&mut result, &[4, 4], &[1, 1], &chunk, &[2, 2], 1);
        let expected = vec![0, 0, 0, 0, 0, b'a', b'b', 0, 0, b'c', b'd', 0, 0, 0, 0, 0];
        assert_eq!(result, expected);
    }

    #[test]
    fn copy_chunk_2d_multi_byte() {
        // 2x2 result, chunk is 1x2 placed at (1, 0). element_size=2
        // Each 2-byte element is little-endian u16; values 100, 200
        let mut result = vec![0u8; 8];
        let chunk = (100u16)
            .to_le_bytes()
            .iter()
            .chain((200u16).to_le_bytes().iter())
            .copied()
            .collect::<Vec<_>>();
        copy_chunk_into_result(&mut result, &[2, 2], &[1, 0], &chunk, &[1, 2], 2);
        // Bytes 4..6 = 100, bytes 6..8 = 200.
        assert_eq!(&result[4..6], &(100u16).to_le_bytes());
        assert_eq!(&result[6..8], &(200u16).to_le_bytes());
    }
}

/// Which dispatch arm `patch_metadata` takes, derived from the body
/// `content-type` field. Mirrors upstream tiled PR #688's two patch modes
/// (an unrecognized/absent content-type is rejected with 406, not dispatched
/// here).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PatchMode {
    /// RFC 6902 ops array (`application/json-patch+json`).
    JsonPatch,
    /// RFC 7396 partial doc (`application/merge-patch+json`).
    MergePatch,
}

/// Apply an RFC 6902 json-patch ops array (`ops`, taken straight from a
/// request body field) directly to `base`, returning the patched document.
/// A null/absent/empty ops list leaves `base` unchanged — mirrors Python's
/// `apply_json_patch(base, field or [])` (server router.py:2345-2347).
fn apply_json_patch_field(
    base: &serde_json::Value,
    ops: Option<&serde_json::Value>,
) -> Result<serde_json::Value, ServerError> {
    let ops = match ops {
        None | Some(serde_json::Value::Null) => return Ok(base.clone()),
        Some(v) => v,
    };
    let ops_array = ops.as_array().ok_or_else(|| {
        ServerError::Validation(
            "application/json-patch+json fields must be JSON arrays of RFC 6902 ops".into(),
        )
    })?;
    if ops_array.is_empty() {
        return Ok(base.clone());
    }
    let patch: json_patch::Patch =
        serde_json::from_value(serde_json::Value::Array(ops_array.clone()))
            .map_err(|e| ServerError::Validation(format!("invalid json-patch: {e}")))?;
    let mut doc = base.clone();
    json_patch::patch(&mut doc, &patch)
        .map_err(|e| ServerError::Validation(format!("json-patch failed: {e}")))?;
    Ok(doc)
}

/// Canonical `(name, version)` identity of a spec for the uniqueness check,
/// mirroring Python's frozen `Spec` dataclass equality (structures/core.py):
/// a bare string `"x"`, `{"name": "x"}`, and `{"name": "x", "version": null}`
/// all collapse to the same identity.
fn spec_identity(spec: &serde_json::Value) -> serde_json::Value {
    if let Some(name) = spec.as_str() {
        serde_json::json!([name, serde_json::Value::Null])
    } else if let Some(obj) = spec.as_object() {
        let name = obj.get("name").cloned().unwrap_or(serde_json::Value::Null);
        let version = obj
            .get("version")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        serde_json::json!([name, version])
    } else {
        spec.clone()
    }
}

/// RFC 7396 merge-patch: recursively merge `patch` into `target`. A
/// `null` value in `patch` deletes the corresponding key on an object.
fn merge_patch_apply(target: &mut serde_json::Value, patch: &serde_json::Value) {
    match (target, patch) {
        (serde_json::Value::Object(target_map), serde_json::Value::Object(patch_map)) => {
            for (key, value) in patch_map {
                if value.is_null() {
                    target_map.remove(key);
                } else if let Some(existing) = target_map.get_mut(key) {
                    merge_patch_apply(existing, value);
                } else {
                    target_map.insert(key.clone(), value.clone());
                }
            }
        }
        (target, patch) => {
            *target = patch.clone();
        }
    }
}

#[cfg(test)]
mod patch_dispatch_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merge_patch_overwrites_scalar() {
        let mut a = json!({"x": 1, "y": 2});
        merge_patch_apply(&mut a, &json!({"y": 99}));
        assert_eq!(a, json!({"x": 1, "y": 99}));
    }

    #[test]
    fn merge_patch_recurses_into_objects() {
        let mut a = json!({"nested": {"a": 1, "b": 2}});
        merge_patch_apply(&mut a, &json!({"nested": {"b": 99}}));
        assert_eq!(a, json!({"nested": {"a": 1, "b": 99}}));
    }

    #[test]
    fn merge_patch_null_deletes_key() {
        let mut a = json!({"x": 1, "y": 2});
        merge_patch_apply(&mut a, &json!({"y": null}));
        assert_eq!(a, json!({"x": 1}));
    }

    #[test]
    fn merge_patch_replaces_arrays_wholesale() {
        let mut a = json!({"arr": [1, 2, 3]});
        merge_patch_apply(&mut a, &json!({"arr": [9]}));
        assert_eq!(a, json!({"arr": [9]}));
    }
}

#[cfg(test)]
mod range_tests {
    use super::parse_range;

    #[test]
    fn full_range() {
        assert_eq!(parse_range("bytes=0-9", 10), Some((0, 9)));
    }

    #[test]
    fn open_ended() {
        assert_eq!(parse_range("bytes=5-", 10), Some((5, 9)));
    }

    #[test]
    fn suffix_last_n() {
        assert_eq!(parse_range("bytes=-3", 10), Some((7, 9)));
    }

    #[test]
    fn out_of_bounds_rejected() {
        assert_eq!(parse_range("bytes=0-99", 10), None);
        assert_eq!(parse_range("bytes=20-30", 10), None);
    }

    #[test]
    fn malformed_rejected() {
        assert_eq!(parse_range("bytes=abc-9", 10), None);
        assert_eq!(parse_range("0-9", 10), None); // missing prefix
    }

    #[test]
    fn multi_range_not_supported() {
        assert_eq!(parse_range("bytes=0-1,3-4", 10), None);
    }
}

#[cfg(test)]
mod sparse_response_tests {
    //! Unit tests for the COO→Arrow column conversion that backs the sparse
    //! read route (`build_sparse_response`). The full HTTP round-trip
    //! (CooAdapter → `/array/block` → client decode) is covered by an
    //! integration test once the sparse data source is wired in.
    use super::*;
    use crate::core::dtype::{BuiltinDType, DynNDArray, Endianness, Kind};

    fn le_dyn<T: Copy>(
        vals: &[T],
        dtype: BuiltinDType,
        to_le: impl Fn(T) -> Vec<u8>,
    ) -> DynNDArray {
        let mut bytes = Vec::new();
        for v in vals {
            bytes.extend_from_slice(&to_le(*v));
        }
        DynNDArray::new(bytes::Bytes::from(bytes), dtype, vec![vals.len()])
    }

    #[test]
    fn dyn_ndarray_to_arrow_decodes_int64_coords_and_float64_data() {
        use arrow::array::{Float64Array, Int64Array};
        let coords = le_dyn(
            &[0i64, 2, 1],
            BuiltinDType::new(Endianness::Little, Kind::Integer, 8),
            |v| v.to_le_bytes().to_vec(),
        );
        let arr = dyn_ndarray_to_arrow(&coords).unwrap();
        let int = arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        assert_eq!(int.values(), &[0i64, 2, 1]);

        let data = le_dyn(
            &[1.5f64, 2.5, 3.5],
            BuiltinDType::new(Endianness::Little, Kind::Float, 8),
            |v| v.to_le_bytes().to_vec(),
        );
        let arr = dyn_ndarray_to_arrow(&data).unwrap();
        let f = arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64Array");
        assert_eq!(f.values(), &[1.5f64, 2.5, 3.5]);
    }

    #[test]
    fn dyn_ndarray_to_arrow_decodes_unsigned_and_narrow_ints() {
        use arrow::array::{Int32Array, UInt16Array};
        let u16s = le_dyn(
            &[7u16, 9],
            BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, 2),
            |v| v.to_le_bytes().to_vec(),
        );
        let arr = dyn_ndarray_to_arrow(&u16s).unwrap();
        assert_eq!(
            arr.as_any()
                .downcast_ref::<UInt16Array>()
                .expect("UInt16Array")
                .values(),
            &[7u16, 9]
        );

        let i32s = le_dyn(
            &[-3i32, 4],
            BuiltinDType::new(Endianness::Little, Kind::Integer, 4),
            |v| v.to_le_bytes().to_vec(),
        );
        let arr = dyn_ndarray_to_arrow(&i32s).unwrap();
        assert_eq!(
            arr.as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32Array")
                .values(),
            &[-3i32, 4]
        );
    }

    /// An element class with no Arrow scalar mapping (e.g. boolean) is rejected
    /// with an internal error rather than panicking or emitting wrong bytes.
    #[test]
    fn dyn_ndarray_to_arrow_rejects_unsupported_dtype() {
        let weird = DynNDArray::new(
            bytes::Bytes::from(vec![1u8]),
            BuiltinDType::new(Endianness::NotApplicable, Kind::Boolean, 1),
            vec![1],
        );
        assert!(dyn_ndarray_to_arrow(&weird).is_err());
    }

    /// Encode named columns as an Arrow IPC file — the inverse of what
    /// `deserialize_sparse_coo` reads.
    fn build_coo_ipc(columns: Vec<(&str, arrow::array::ArrayRef)>) -> Vec<u8> {
        use arrow::datatypes::{Field, Schema};
        let fields: Vec<Field> = columns
            .iter()
            .map(|(n, c)| Field::new(*n, c.data_type().clone(), false))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<arrow::array::ArrayRef> = columns.into_iter().map(|(_, c)| c).collect();
        let batch = arrow::array::RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
        let mut buf = Vec::new();
        {
            let mut w = arrow::ipc::writer::FileWriter::try_new(&mut buf, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        buf
    }

    fn decode_i64(arr: &DynNDArray) -> Vec<i64> {
        arr.data
            .chunks_exact(8)
            .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
            .collect()
    }

    fn decode_f64(arr: &DynNDArray) -> Vec<f64> {
        arr.data
            .chunks_exact(8)
            .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
            .collect()
    }

    /// COO round-trip: `dim{i}` columns of mixed integer width normalize to
    /// int64-LE, the `data` column keeps its float64 dtype, and every value
    /// survives.
    #[test]
    fn deserialize_sparse_coo_roundtrips_and_normalizes_coords() {
        use arrow::array::{Float64Array, Int32Array, Int64Array};
        // dim0 as Int64, dim1 as narrow Int32 — both must land as int64-LE.
        let body = build_coo_ipc(vec![
            ("dim0", Arc::new(Int64Array::from(vec![0i64, 2])) as _),
            ("dim1", Arc::new(Int32Array::from(vec![1i32, 0])) as _),
            ("data", Arc::new(Float64Array::from(vec![1.5f64, 3.7])) as _),
        ]);
        let sd = deserialize_sparse_coo(&body, 2).unwrap();
        assert_eq!(sd.coords.len(), 2);
        for c in &sd.coords {
            assert_eq!(c.dtype.kind, Kind::Integer);
            assert_eq!(c.dtype.element_size(), 8);
        }
        assert_eq!(decode_i64(&sd.coords[0]), vec![0, 2]);
        assert_eq!(decode_i64(&sd.coords[1]), vec![1, 0]);
        assert_eq!(sd.data.dtype.kind, Kind::Float);
        assert_eq!(sd.data.dtype.element_size(), 8);
        assert_eq!(decode_f64(&sd.data), vec![1.5, 3.7]);
    }

    /// A `data` column dtype outside the sparse parquet writer's set
    /// (float64/float32/int64/int32) is rejected at the deserialize boundary.
    #[test]
    fn deserialize_sparse_coo_rejects_unsupported_data_dtype() {
        use arrow::array::{Int64Array, UInt64Array};
        let body = build_coo_ipc(vec![
            ("dim0", Arc::new(Int64Array::from(vec![0i64])) as _),
            ("data", Arc::new(UInt64Array::from(vec![5u64])) as _),
        ]);
        assert!(deserialize_sparse_coo(&body, 1).is_err());
    }

    /// A body missing a required `dim{i}` column for the node's dimensionality
    /// is a client error, not a panic.
    #[test]
    fn deserialize_sparse_coo_rejects_missing_dim_column() {
        use arrow::array::{Float64Array, Int64Array};
        // ndim=2 but only dim0 present.
        let body = build_coo_ipc(vec![
            ("dim0", Arc::new(Int64Array::from(vec![0i64])) as _),
            ("data", Arc::new(Float64Array::from(vec![1.5f64])) as _),
        ]);
        assert!(deserialize_sparse_coo(&body, 2).is_err());
    }

    /// A value dtype that differs from the node's declared `data_type` is
    /// rejected — the read path would otherwise mis-size and reinterpret the
    /// stored bytes. A matching dtype passes.
    #[test]
    fn ensure_sparse_data_dtype_rejects_mismatch_accepts_match() {
        use crate::core::adapters::SparseData;
        use crate::core::dtype::DType;
        use crate::core::structures::SparseStructure;

        let f64_dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
        let f32_dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 4);
        let structure = SparseStructure {
            chunks: vec![vec![1]],
            shape: vec![1],
            data_type: Some(DType::Builtin(f64_dtype.clone())),
            ..Default::default()
        };
        let mk = |dt: BuiltinDType, bytes: Vec<u8>| SparseData {
            coords: vec![DynNDArray::new(
                bytes::Bytes::from(0i64.to_le_bytes().to_vec()),
                BuiltinDType::new(Endianness::Little, Kind::Integer, 8),
                vec![1],
            )],
            data: DynNDArray::new(bytes::Bytes::from(bytes), dt, vec![1]),
        };
        assert!(
            ensure_sparse_data_dtype(
                &structure,
                &mk(f64_dtype.clone(), 1.5f64.to_le_bytes().to_vec())
            )
            .is_ok()
        );
        assert!(
            ensure_sparse_data_dtype(&structure, &mk(f32_dtype, 1.5f32.to_le_bytes().to_vec()))
                .is_err()
        );
    }
}
