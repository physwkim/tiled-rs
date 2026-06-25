//! Route handlers for the Tiled API.

use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::Arc;

use axum::Json;
use axum::extract::{OriginalUri, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use serde::Deserialize;

use crate::extractors::PathSegments;

use tiled_core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_core::links;
use tiled_core::schemas::{About, AboutAuthentication, Response, SortDirection};

use crate::core;
use crate::error::ServerError;
use crate::extractors::BaseUrl;
use crate::state::AppState;

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
    auth: crate::AuthContext,
    segments: &[String],
    required_scope: tiled_auth::Scope,
) -> Result<crate::AuthContext, ServerError> {
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
async fn resolve_entry_catalog(
    state: &AppState,
    mut auth: crate::AuthContext,
    segments: &[String],
) -> Result<crate::AuthContext, ServerError> {
    let catalog = state
        .catalog
        .as_ref()
        .expect("resolve_entry_catalog requires catalog");
    for i in 0..segments.len() {
        let prefix = &segments[..=i];
        let node = catalog
            .lookup(prefix)
            .await
            .map_err(map_catalog_err)?
            .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;
        auth = auth
            .narrow_for_node(
                state.access_policy.as_deref(),
                tiled_access::NodeContext {
                    path: prefix,
                    structure_family: &node.structure_family,
                    metadata: &node.metadata,
                    access_blob: &node.access_blob,
                },
            )
            .await;
        if !auth.scopes.contains(tiled_auth::Scope::ReadMetadata) {
            return Err(ServerError::NotFound(format!(
                "'{}' not found",
                segments.join("/")
            )));
        }
    }
    Ok(auth)
}

/// In-memory tree path: walk the tree to verify existence (the async
/// `ContainerAdapter` resolves each hop on the executor), then narrow at the
/// terminal node with empty access_blob (in-memory adapters carry no per-node
/// blob).
async fn resolve_entry_tree(
    state: &AppState,
    auth: crate::AuthContext,
    segments: &[String],
) -> Result<crate::AuthContext, ServerError> {
    let adapter = core::walk_tree(state.root_tree.as_ref(), segments).await?;
    let sf = adapter.structure_family().to_string();
    let metadata = adapter.metadata().clone();

    let access_blob = serde_json::Value::Object(Default::default());
    let narrowed = auth
        .narrow_for_node(
            state.access_policy.as_deref(),
            tiled_access::NodeContext {
                path: segments,
                structure_family: &sf,
                metadata: &metadata,
                access_blob: &access_blob,
            },
        )
        .await;
    if !narrowed.scopes.contains(tiled_auth::Scope::ReadMetadata) {
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

    // Surface configured authenticators so the SPA can render the right
    // login form. We only advertise internal (username/password) authenticators
    // here — `state.external_oidc` is a *bearer validator*, not an OAuth code-
    // flow initiator: it accepts tokens issued elsewhere but doesn't drive a
    // browser redirect login. Emitting it as a `mode=external` provider would
    // be a lie until upstream tiled #1178's `/authorize` endpoint is ported
    // (tracked in workspace task; needs OidcProvider to gain client_id/secret/
    // authorize_endpoint/token_endpoint).
    let providers: Vec<serde_json::Value> = state
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
    // Python: `authentication.required = not settings.allow_anonymous_access`
    // (router.py:301) — purely the anonymous-access policy, NOT whether login
    // providers exist. Rust's `no_auth_configured()` (no api_key AND no
    // auth_db) is the single source of truth for "anonymous access allowed",
    // so it is exactly Rust's `allow_anonymous_access`. Negating it fixes the
    // previous `!providers.is_empty() || external_oidc.is_some()`, which
    // misreported a single-user `api_key`-only server (no authenticators) as
    // NOT requiring auth even though every request needs the key.
    let auth_required = !state.no_auth_configured();

    let about = About {
        api_version: 0,
        library_version: env!("CARGO_PKG_VERSION").to_string(),
        formats,
        aliases,
        queries: state.query_names.clone(),
        authentication: AboutAuthentication {
            required: auth_required,
            providers,
            links: None,
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
    auth: crate::AuthContext,
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
/// `specs`, `sorting`, `access_blob`, `data_sources`. `count` and the empty
/// value (`none`) request no attribute section; unknown names are ignored.
///
/// The caller MUST invoke this only when the client actually sent `fields` —
/// an absent `fields` means "full entry" (the FastAPI default selects every
/// `EntryFields`), which is the unpruned state and must not be pruned to nothing.
fn prune_entry_fields(attrs: &mut tiled_core::schemas::NodeAttributes, requested: &[String]) {
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
    if !want("data_sources") {
        attrs.data_sources = None;
    }
}

pub async fn metadata(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    BaseUrl(base_url): BaseUrl,
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::ReadMetadata)?;
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
            tiled_auth::Scope::ReadMetadata,
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
        catalog_metadata_resource(catalog, &segments, &base_url, include_data_sources).await?
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
        resource.links = tiled_core::schemas::NodeLinks::default();
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
    auth: crate::AuthContext,
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
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::ReadMetadata)?;
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
    let mut queries = tiled_core::queries::decode_query_filters(&filter_params)?;

    // Per-ancestor auth gate on the parent container path.
    // Returns 404 (not 403) when any ancestor's per-node policy drops
    // ReadMetadata — same behaviour as the metadata read gate.
    let auth = if !segments.is_empty() {
        resolve_entry(&state, auth, &segments, tiled_auth::Scope::ReadMetadata).await?
    } else {
        auth
    };

    // Inject the access-policy list filter so the SQL/in-memory path
    // only returns nodes the principal is permitted to see. A listing/search
    // needs read:metadata (parity with Python get_entry's filter_for_access
    // scopes=["read:metadata"], dependencies.py:78).
    if let Some(ref policy) = state.access_policy {
        let principal_ref = auth.principal.as_deref();
        let requested = tiled_auth::ScopeSet::from_iter([tiled_auth::Scope::ReadMetadata]);
        if let Some(f) = policy
            .list_filter(principal_ref, &auth.scopes, &requested)
            .await
        {
            queries.insert(0, tiled_core::queries::Query::AccessBlobFilter(f));
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
            item.links = tiled_core::schemas::NodeLinks::default();
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
    auth: crate::AuthContext,
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
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::ReadMetadata)?;
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
    let mut queries = tiled_core::queries::decode_query_filters(&filter_params)?;

    // Per-ancestor auth gate, identical to search/metadata reads (404, not 403,
    // when an ancestor's per-node policy drops ReadMetadata).
    let auth = if !segments.is_empty() {
        resolve_entry(&state, auth, &segments, tiled_auth::Scope::ReadMetadata).await?
    } else {
        auth
    };

    // Scope to the nodes the principal may see — the same list filter the
    // search path injects, so distinct counts only permitted children.
    if let Some(ref policy) = state.access_policy {
        let principal_ref = auth.principal.as_deref();
        let requested = tiled_auth::ScopeSet::from_iter([tiled_auth::Scope::ReadMetadata]);
        if let Some(f) = policy
            .list_filter(principal_ref, &auth.scopes, &requested)
            .await
        {
            queries.insert(0, tiled_core::queries::Query::AccessBlobFilter(f));
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
    family: tiled_core::structures::StructureFamily,
    requested: &str,
    registry: &tiled_serialization::SerializationRegistry,
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
fn map_serialize_error(e: tiled_serialization::SerializeError) -> ServerError {
    if let Some(shape) = e.downcast_ref::<tiled_serialization::UnsupportedShape>() {
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
    data: tiled_core::dtype::DynNDArray,
    format_param: Option<&str>,
    headers: &HeaderMap,
    state: &AppState,
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

    let family = tiled_core::structures::StructureFamily::Array;
    let media_type = tiled_serialization::negotiate_media_type(
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

    Ok(serve_with_range(headers, &media_type, body))
}

// Shared by `table_partition` and `table_full`: encode an already-read
// `ArrowTable` to Arrow IPC and route it through the serialization registry so
// format negotiation applies (e.g. csv/parquet re-encode the IPC bytes).
async fn build_table_response(
    table: tiled_core::dtype::ArrowTable,
    format_param: Option<&str>,
    headers: &HeaderMap,
    state: &AppState,
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
    let family = tiled_core::structures::StructureFamily::Table;
    let media_type = tiled_serialization::negotiate_media_type(
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
    let body =
        tokio::task::spawn_blocking(move || serializer(&ipc_bytes, &serde_json::Value::Null))
            .await
            .map_err(|e| ServerError::Internal(format!("serialize task failed: {e}")))?
            .map_err(map_serialize_error)?;

    Ok(serve_with_range(headers, &media_type, body))
}

/// Convert a `DynNDArray` (raw little-endian element bytes + dtype) into an
/// Arrow array, dispatching on the dtype class — the inverse of the array
/// adapters' `to_le_bytes` emission. Used to assemble the COO columns of a
/// sparse response.
fn dyn_ndarray_to_arrow(
    arr: &tiled_core::dtype::DynNDArray,
) -> Result<arrow::array::ArrayRef, ServerError> {
    use arrow::array::{
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    use tiled_core::dtype::Kind;
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

// Shared by the sparse branches of `array_block` and `array_full`: encode a
// `SparseData` (COO coordinates + values) as a table with columns
// `dim0..dim{ndim-1}` and `data`, then route it through the serialization
// registry under the `Sparse` family — mirroring Python `serialization/sparse.py`
// (`to_dataframe`: one `dim{i}` column per coordinate axis plus `data`). The
// default `application/vnd.apache.arrow.file` serializer emits the Arrow IPC
// bytes verbatim; `?format=`/`Accept` can re-encode to CSV/parquet/etc.
async fn build_sparse_response(
    sparse: tiled_core::adapters::SparseData,
    format_param: Option<&str>,
    headers: &HeaderMap,
    state: &AppState,
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

    let family = tiled_core::structures::StructureFamily::Sparse;
    let accept = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let media_type = tiled_serialization::negotiate_media_type(
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

    Ok(serve_with_range(headers, &media_type, body))
}

// Shared by `ragged_full`: encode an already-read `RaggedData` through the
// serialization registry so format negotiation applies (JSON list-of-lists,
// Awkward zip-of-buffers, and — feature-gated — Arrow IPC / Parquet). Mirrors
// Python `construct_data_response` for the ragged family (router.py:890-902).
async fn build_ragged_response(
    data: tiled_core::adapters::RaggedData,
    format_param: Option<&str>,
    headers: &HeaderMap,
    state: &AppState,
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

    let family = tiled_core::structures::StructureFamily::Ragged;
    let accept = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let media_type = tiled_serialization::negotiate_media_type(
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

    Ok(serve_with_range(headers, &media_type, body))
}

// ---------------------------------------------------------------------------
// GET /api/v1/array/block/{*path}
// ---------------------------------------------------------------------------

pub async fn array_block(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/array/block/");
    // H2: per-node policy check.
    let _ = resolve_entry(&state, auth.clone(), &segments, tiled_auth::Scope::ReadData).await?;

    let block_str = params
        .get("block")
        .map(|s| s.to_string())
        .unwrap_or_default();
    let slice_str = params
        .get("slice")
        .map(|s| s.to_string())
        .unwrap_or_default();
    let format_str = params.get("format").map(|s| s.to_string());
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
        return build_sparse_response(data, format_str.as_deref(), &headers, &state).await;
    }

    let array_adapter: Arc<dyn tiled_core::adapters::ArrayAdapterRead> =
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
        "" => tiled_core::ndslice::NDSlice::empty(),
        s => tiled_core::ndslice::NDSlice::from_numpy_str(s)
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

    build_array_response(data, format_str.as_deref(), &headers, &state).await
}

/// Build a Response that honors `Range: bytes=...` when present
/// (upstream tiled PR #762). Used by data routes that produce a full
/// byte buffer in memory — DuckDB httpfs and similar tools rely on
/// partial GETs to scan only the file slices they need.
fn serve_with_range(
    headers: &HeaderMap,
    content_type: &str,
    body: bytes::Bytes,
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
    match range {
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
    }
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
// PATCH /api/v1/array/full/{*path}?append_along=N — extend an array
// ---------------------------------------------------------------------------
//
// Upstream tiled PR #802. Body is the raw bytes to append along axis
// `append_along` (default 0). The route forwards to
// `ArrayAdapterWrite::append`; adapters that don't override the trait
// default get a 422 explaining "not supported by this adapter". The
// resulting axis length is published on the streaming bus as a
// `data-appended` event so subscribers see the new shape live.

pub async fn array_append(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    auth: crate::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/array/full/");
    // H2: per-node policy check (matches every other data handler).
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        tiled_auth::Scope::WriteData,
    )
    .await?;

    let append_along: usize = params
        .get("append_along")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // The async tree walk resolves each hop on the executor and hands back an
    // owned `Arc` clone of the leaf; validation + the async append then run on
    // the executor (a writable store that offloads `append` does so internally).
    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
    let array_adapter: Arc<dyn tiled_core::adapters::ArrayAdapterRead> =
        adapter.as_array_arc().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not an array", segments.join("/")))
        })?;

    let writable = array_adapter.as_writable().ok_or_else(|| {
        ServerError::Validation(
            "this array adapter does not support append; only adapters whose \
                 underlying store can grow (zarr, ND-streaming) implement it"
                .into(),
        )
    })?;
    let structure = array_adapter.structure();
    if append_along >= structure.shape.len() {
        return Err(ServerError::Validation(format!(
            "append_along={append_along} out of range (ndim={})",
            structure.shape.len()
        )));
    }
    // Construct a DynNDArray view over the request body. Shape is the
    // existing structure's shape with `shape[append_along]` swapped for
    // the inferred number of new elements (body bytes / element size /
    // product of remaining dims).
    let elem_size = match &structure.data_type {
        tiled_core::dtype::DType::Builtin(b) => b.element_size(),
        _ => {
            return Err(ServerError::Validation(
                "append: only Builtin dtypes are supported".into(),
            ));
        }
    };
    let mut new_shape = structure.shape.clone();
    let other_axes: usize = structure
        .shape
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != append_along)
        .map(|(_, d)| *d)
        .product();
    if elem_size == 0 || other_axes == 0 {
        return Err(ServerError::Validation(
            "append: structure has zero-element-size or zero-area cross-section".into(),
        ));
    }
    let row_bytes = other_axes * elem_size;
    if !body.len().is_multiple_of(row_bytes) {
        return Err(ServerError::Validation(format!(
            "append: body length {} is not a multiple of cross-section bytes {row_bytes}",
            body.len()
        )));
    }
    let added_along_axis = body.len() / row_bytes;
    new_shape[append_along] = added_along_axis;
    let dtype = match &structure.data_type {
        tiled_core::dtype::DType::Builtin(b) => b.clone(),
        _ => unreachable!("checked above"),
    };
    let payload = tiled_core::dtype::DynNDArray::new(body, dtype, new_shape);
    let new_axis_len = writable
        .append(payload, append_along)
        .await
        .map_err(ServerError::from)?;

    // Keep the catalog structure in sync with the grown store, so GET /metadata
    // reports the new shape (Python tiled updates the DB on append too). Rather
    // than recompute the chunk grid here, re-resolve and read the authoritative
    // structure back from the store.
    if let Some(catalog) = &state.catalog
        && let Some(node) = catalog.lookup(&segments).await.map_err(map_catalog_err)?
    {
        let data_sources = catalog
            .list_data_sources(node.id)
            .await
            .map_err(map_catalog_err)?;
        if let Some(ds) = data_sources.first() {
            let fresh = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
            if let Some(fresh_array) = fresh.as_array_arc() {
                let new_structure = serde_json::to_value(
                    tiled_core::structures::AnyStructure::Array(fresh_array.structure().clone()),
                )
                .map_err(|e| ServerError::Internal(format!("serialize structure: {e}")))?;
                catalog
                    .update_data_source(ds.id, new_structure, ds.parameters.clone())
                    .await
                    .map_err(map_catalog_err)?;
            }
        }
    }

    let path = segments.join("/");
    state.streaming_bus.publish(
        &path,
        crate::streaming::UpdateKind::DataAppended {
            partition: Some(append_along),
        },
    );
    Ok(Json(serde_json::json!({
        "axis": append_along,
        "new_size": new_axis_len,
    })))
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
    auth: crate::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/array/full/");
    // Per-node policy check, same as every other data handler.
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        tiled_auth::Scope::WriteData,
    )
    .await?;

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
    let array_adapter: Arc<dyn tiled_core::adapters::ArrayAdapterRead> =
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
    let dtype = match &structure.data_type {
        tiled_core::dtype::DType::Builtin(b) => b.clone(),
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
    let shape = structure.shape.clone();
    let payload = tiled_core::dtype::DynNDArray::new(body, dtype, shape);
    writable.write(payload).await.map_err(ServerError::from)?;

    let path = segments.join("/");
    // Signal subscribers that the node's data changed (closest existing event).
    state.streaming_bus.publish(
        &path,
        crate::streaming::UpdateKind::DataAppended { partition: None },
    );
    Ok(Json(serde_json::json!({ "ok": true })))
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
    auth: crate::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/array/block/");
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        tiled_auth::Scope::WriteData,
    )
    .await?;

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
    let array_adapter: Arc<dyn tiled_core::adapters::ArrayAdapterRead> =
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
    let dtype = match &structure.data_type {
        tiled_core::dtype::DType::Builtin(b) => b.clone(),
        _ => {
            return Err(ServerError::Validation(
                "array write: only builtin (non-struct) dtypes are supported".into(),
            ));
        }
    };

    // `?block=` is one index per axis (no ranges: a write targets exactly one
    // chunk). Empty defaults to the origin chunk.
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
    let elem = dtype.element_size();
    let expected = chunk_shape.iter().product::<usize>() * elem;
    if body.len() != expected {
        return Err(ServerError::Validation(format!(
            "array block write: body is {} bytes but chunk {block:?} needs {expected} \
             (chunk shape {chunk_shape:?}, {elem}-byte elements)",
            body.len()
        )));
    }
    let payload = tiled_core::dtype::DynNDArray::new(body, dtype, chunk_shape);
    writable
        .write_block(payload, &block)
        .await
        .map_err(ServerError::from)?;

    let path = segments.join("/");
    state.streaming_bus.publish(
        &path,
        crate::streaming::UpdateKind::DataAppended { partition: None },
    );
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
        p
    }
}

pub async fn array_full_post(
    state: State<AppState>,
    BaseUrl(base_url): BaseUrl,
    headers: HeaderMap,
    auth: crate::AuthContext,
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
    auth: crate::AuthContext,
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
    auth: crate::AuthContext,
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
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/container/full/");
    // H2: per-node policy check.
    if !segments.is_empty() {
        let _ = resolve_entry(&state, auth.clone(), &segments, tiled_auth::Scope::ReadData).await?;
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
    // Resolve effective media type once: format param beats Accept header. An
    // explicit but unserviceable format/Accept resolves to `None` → HTTP 406,
    // consistent with the array/table/sparse handlers (no silent HTML fallback).
    let family = tiled_core::structures::StructureFamily::Container;
    let media_type = tiled_serialization::negotiate_media_type(
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
        let requested = tiled_auth::ScopeSet::from_iter([tiled_auth::Scope::ReadData]);
        policy
            .list_filter(auth.principal.as_deref(), &auth.scopes, &requested)
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
        collect_zip_entries(
            container,
            "",
            &path,
            access_filter.as_ref(),
            &mut entries,
            0,
            max_depth,
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
        for ZipEntry { name, leaf } in entries {
            match leaf {
                ZipLeaf::Array(arc) => {
                    let slice = tiled_core::ndslice::NDSlice::empty();
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
    let body_json = if media_type == tiled_core::media_type::mime::JSON {
        // Root node: the container's own metadata plus the recursively-built
        // child tree. The access filter is applied at every level inside the
        // helper (parity with Python's per-node `filter_for_access`).
        let contents = build_container_json_contents(container, access_filter.as_ref()).await?;
        let mut tree = serde_json::Map::new();
        tree.insert("contents".into(), serde_json::Value::Object(contents));
        tree.insert("metadata".into(), container.metadata().clone());
        serde_json::to_vec(&serde_json::Value::Object(tree))
            .map_err(|e| ServerError::Internal(format!("encode: {e}")))?
    } else {
        // H3: apply access filter to listing.
        let queries: Vec<tiled_core::queries::Query> = access_filter
            .map(|f| vec![tiled_core::queries::Query::AccessBlobFilter(f)])
            .unwrap_or_default();
        let visible_keys = if queries.is_empty() {
            container.keys().await?
        } else {
            // An unsupported query variant propagates as HTTP 400.
            container.search(&queries).await?
        };
        let mut children: Vec<tiled_core::schemas::Resource> = Vec::new();
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
        tiled_core::structures::StructureFamily::Container,
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

    Ok(serve_with_range(&headers, &media_type, body))
}

/// One leaf to bundle into a deep-export zip. Phase 1 captures only an OWNED
/// Arc handle (no decoded data) so the read can run on the executor in phase 2.
enum ZipLeaf {
    Array(Arc<dyn tiled_core::adapters::ArrayAdapterRead>),
    Table(Arc<dyn tiled_core::adapters::TableAdapterRead>),
    /// Pre-serialized `.json` crumb for families not yet bundled
    /// (Sparse/Awkward). No read is needed, so the bytes are captured directly.
    Crumb(Vec<u8>),
}

/// One ordered entry in the deep-export zip.
struct ZipEntry {
    /// Full in-zip filename including extension (`.bin` / `.arrow` / `.json`).
    name: String,
    leaf: ZipLeaf,
}

/// The owned-buffer ZipWriter that ping-pongs through `spawn_blocking` in
/// phase 2 (one write per leaf, returned for the next iteration).
type ZipBuf = zip::ZipWriter<Cursor<Vec<u8>>>;

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
fn collect_zip_entries<'a>(
    container: &'a dyn ContainerAdapter,
    prefix: &'a str,
    base_path: &'a str,
    access_filter: Option<&'a tiled_core::queries::AccessBlobFilter>,
    out: &'a mut Vec<ZipEntry>,
    current_depth: usize,
    max_depth: Option<usize>,
) -> tiled_core::adapters::BoxFuture<'a, Result<(), ServerError>> {
    Box::pin(async move {
        let visible_keys = match access_filter {
            // AccessBlobFilter is supported by the catalog/map adapters used
            // here; an adapter that cannot evaluate it propagates HTTP 400.
            Some(f) => {
                container
                    .search(&[tiled_core::queries::Query::AccessBlobFilter(f.clone())])
                    .await?
            }
            None => container.keys().await?,
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
            if let Some(arc) = child.as_array_arc() {
                out.push(ZipEntry {
                    name: format!("{entry_name}.bin"),
                    leaf: ZipLeaf::Array(arc),
                });
            } else if let Some(arc) = child.as_table_arc() {
                out.push(ZipEntry {
                    name: format!("{entry_name}.arrow"),
                    leaf: ZipLeaf::Table(arc),
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
                    });
                } else {
                    collect_zip_entries(
                        child_c,
                        &entry_name,
                        base_path,
                        access_filter,
                        out,
                        current_depth + 1,
                        max_depth,
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
    access_filter: Option<&'a tiled_core::queries::AccessBlobFilter>,
) -> tiled_core::adapters::BoxFuture<
    'a,
    Result<serde_json::Map<String, serde_json::Value>, ServerError>,
> {
    Box::pin(async move {
        let visible_keys = match access_filter {
            Some(f) => {
                container
                    .search(&[tiled_core::queries::Query::AccessBlobFilter(f.clone())])
                    .await?
            }
            None => container.keys().await?,
        };
        let mut contents = serde_json::Map::new();
        for key in visible_keys {
            let Some(child) = container.get(&key).await? else {
                continue;
            };
            let node = match child.structure_family() {
                tiled_core::structures::StructureFamily::Container => {
                    let sub = build_container_json_contents(
                        child
                            .as_container()
                            .expect("container family => as_container"),
                        access_filter,
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
                tiled_core::structures::StructureFamily::Table => {
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
    adapter: &dyn tiled_core::adapters::ArrayAdapterRead,
    block_specs: &[BlockSpec],
) -> Result<tiled_core::dtype::DynNDArray, ServerError> {
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
        .read_block(&first_idx, &tiled_core::ndslice::NDSlice::empty())
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
                .read_block(&chunk_global_idx, &tiled_core::ndslice::NDSlice::empty())
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

    Ok(tiled_core::dtype::DynNDArray::new(
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
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/array/full/");
    let _ = resolve_entry(&state, auth.clone(), &segments, tiled_auth::Scope::ReadData).await?;

    let slice_str = params
        .get("slice")
        .map(|s| s.to_string())
        .unwrap_or_default();
    let format_str = params.get("format").map(|s| s.to_string());
    // The async tree walk resolves each hop on the executor and hands back an
    // owned `Arc` clone of the leaf; the read future offloads its own blocking
    // and is awaited on the executor below.
    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;

    let slice = match slice_str.as_str() {
        "" => tiled_core::ndslice::NDSlice::empty(),
        s => tiled_core::ndslice::NDSlice::from_numpy_str(s)
            .map_err(|e| ServerError::Validation(format!("Invalid slice '{s}': {e}")))?,
    };

    // Sparse leaf: serve the full COO table (Python serialization/sparse.py),
    // applying the optional `?slice=` via the adapter's `read`.
    if let Some(sparse) = adapter.as_sparse_arc() {
        let data = sparse.read(&slice).await.map_err(ServerError::from)?;
        return build_sparse_response(data, format_str.as_deref(), &headers, &state).await;
    }

    let array_adapter: Arc<dyn tiled_core::adapters::ArrayAdapterRead> =
        adapter.as_array_arc().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not an array", segments.join("/")))
        })?;

    let data = array_adapter
        .read(&slice)
        .await
        .map_err(ServerError::from)?;

    build_array_response(data, format_str.as_deref(), &headers, &state).await
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
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/ragged/full/");
    let _ = resolve_entry(&state, auth.clone(), &segments, tiled_auth::Scope::ReadData).await?;

    let slice_str = params
        .get("slice")
        .map(|s| s.to_string())
        .unwrap_or_default();
    let format_str = params.get("format").map(|s| s.to_string());
    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;

    let slice = match slice_str.as_str() {
        "" => tiled_core::ndslice::NDSlice::empty(),
        s => tiled_core::ndslice::NDSlice::from_numpy_str(s)
            .map_err(|e| ServerError::Validation(format!("Invalid slice '{s}': {e}")))?,
    };

    let ragged = adapter.as_ragged_arc().ok_or_else(|| {
        ServerError::WrongType(format!("'{}' is not a ragged array", segments.join("/")))
    })?;

    // A slice Awkward cannot apply (out-of-bounds row index, scalar reduction,
    // index past a short ragged row) is a client error → 422, matching Python's
    // `RaggedSlicingError` handling (router.py:873-880), not a 500.
    let data = ragged.read(&slice).await.map_err(|e| match e {
        tiled_core::TiledError::InvalidSlice(msg) => ServerError::Validation(format!(
            "Cannot apply the requested slice to the given ragged array: {msg}. \
             Try reading the entire array and slicing it on the client side instead."
        )),
        other => ServerError::from(other),
    })?;

    build_ragged_response(data, format_str.as_deref(), &headers, &state).await
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
    auth: crate::AuthContext,
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

    table_partition_core(
        &state,
        &auth,
        &segments,
        partition,
        fields,
        format_param,
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
    auth: crate::AuthContext,
    columns: Option<Json<Vec<String>>>,
) -> Result<impl IntoResponse, ServerError> {
    let segments = segments_from_uri(&uri, "/api/v1/table/partition/");
    let partition = table_partition_index(&params);
    let format_param = table_partition_format(&params);
    let fields = columns.map(|Json(c)| c).filter(|c| !c.is_empty());

    table_partition_core(
        &state,
        &auth,
        &segments,
        partition,
        fields,
        format_param,
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

/// Shared read+respond core for the GET and POST `table/partition` handlers.
/// The two differ only in where the column projection comes from (repeated
/// query params vs a JSON body); auth, the per-node policy check, the tree
/// walk, the bounds check, and response negotiation are identical.
async fn table_partition_core(
    state: &AppState,
    auth: &crate::AuthContext,
    segments: &[String],
    partition: usize,
    fields: Option<Vec<String>>,
    format_param: Option<String>,
    headers: &HeaderMap,
) -> Result<axum::response::Response, ServerError> {
    auth.require(tiled_auth::Scope::ReadData)?;
    // H2: per-node policy check.
    let _ = resolve_entry(state, auth.clone(), segments, tiled_auth::Scope::ReadData).await?;

    // The async tree walk resolves each hop on the executor and hands back an
    // owned `Arc` leaf; the partition read future offloads its own blocking and
    // is awaited on the executor; the Arrow IPC encode is offloaded on its own.
    let adapter = core::walk_tree(state.root_tree.as_ref(), segments).await?;
    let table_adapter: Arc<dyn tiled_core::adapters::TableAdapterRead> =
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

    build_table_response(table, format_param.as_deref(), headers, state).await
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
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/table/full/");
    // H2: per-node policy check.
    let _ = resolve_entry(&state, auth.clone(), &segments, tiled_auth::Scope::ReadData).await?;

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

    // The async tree walk resolves each hop on the executor and hands back an
    // owned `Arc` clone of the leaf; the read future offloads its own blocking
    // and is awaited on the executor below (see `table_partition`).
    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
    let table_adapter: Arc<dyn tiled_core::adapters::TableAdapterRead> =
        adapter.as_table_arc().ok_or_else(|| {
            ServerError::WrongType(format!("'{}' is not a table", segments.join("/")))
        })?;

    let table = table_adapter
        .read(fields.as_deref())
        .await
        .map_err(ServerError::from)?;

    build_table_response(table, format_param.as_deref(), &headers, &state).await
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
}

pub async fn table_full_post(
    state: State<AppState>,
    headers: HeaderMap,
    auth: crate::AuthContext,
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

    table_full(state, OriginalUri(uri), Query(query), headers, auth)
        .await
        .map(IntoResponse::into_response)
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
    auth: crate::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/table/full/");
    // Per-node policy check, same as every other data handler.
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        tiled_auth::Scope::WriteData,
    )
    .await?;

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
    let table_adapter: Arc<dyn tiled_core::adapters::TableAdapterRead> =
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

    let path = segments.join("/");
    // Signal subscribers that the node's data changed (closest existing event).
    state.streaming_bus.publish(
        &path,
        crate::streaming::UpdateKind::DataAppended { partition: None },
    );
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// Validate that `table`'s column names match the node's declared columns.
/// Returns a 422 error when they disagree, preventing a later read from
/// encountering a schema that contradicts the catalog structure.
fn validate_table_columns(
    table: &tiled_core::dtype::ArrowTable,
    structure: &tiled_core::structures::TableStructure,
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
fn decode_arrow_ipc_table(body: &[u8]) -> Result<tiled_core::dtype::ArrowTable, ServerError> {
    use arrow::ipc::reader::FileReader;
    let cursor = std::io::Cursor::new(body.to_vec());
    let reader = FileReader::try_new(cursor, None)
        .map_err(|e| ServerError::Validation(format!("invalid Arrow IPC table body: {e}")))?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    for b in reader {
        batches.push(b.map_err(|e| ServerError::Validation(format!("Arrow IPC batch: {e}")))?);
    }
    Ok(tiled_core::dtype::ArrowTable { batches, schema })
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
    auth: crate::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/table/partition/");
    let partition = table_partition_index(&params);
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        tiled_auth::Scope::WriteData,
    )
    .await?;

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
    let table_adapter: Arc<dyn tiled_core::adapters::TableAdapterRead> =
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

    let path = segments.join("/");
    state.streaming_bus.publish(
        &path,
        crate::streaming::UpdateKind::DataAppended {
            partition: Some(partition),
        },
    );
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
    auth: crate::AuthContext,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::WriteData)?;
    let segments = segments_from_uri(&uri, "/api/v1/table/partition/");
    let partition = table_partition_index(&params);
    let _ = resolve_entry(
        &state,
        auth.clone(),
        &segments,
        tiled_auth::Scope::WriteData,
    )
    .await?;

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments).await?;
    let table_adapter: Arc<dyn tiled_core::adapters::TableAdapterRead> =
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

    let path = segments.join("/");
    state.streaming_bus.publish(
        &path,
        crate::streaming::UpdateKind::DataAppended {
            partition: Some(partition),
        },
    );
    Ok(Json(serde_json::json!({ "ok": true })))
}

// ---------------------------------------------------------------------------
// GET /documents/{*path} — Stream Bluesky documents (databroker compat)
// ---------------------------------------------------------------------------

pub async fn get_documents(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/documents/");
    if segments.is_empty() {
        return Err(ServerError::Validation(
            "Path to a BlueskyRun is required".into(),
        ));
    }
    // H2: per-node policy check.
    let _ = resolve_entry(&state, auth.clone(), &segments, tiled_auth::Scope::ReadData).await?;

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
    auth: crate::AuthContext,
    body: Json<tiled_core::schemas::PostMetadataRequest>,
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
    auth: crate::AuthContext,
    Json(req): Json<tiled_core::schemas::PostMetadataRequest>,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::WriteMetadata)?;
    auth.require(tiled_auth::Scope::CreateNode)?;
    auth.require(tiled_auth::Scope::Register)?;
    let segments = create_segments_from_uri(&uri);
    // Register trusts the client-supplied assets (existing data); it does not
    // generate storage.
    create_node_core(state, segments, base_url, auth, req, false).await
}

// POST /api/v1/metadata/ — root variant of the asset-free creation alias.
pub async fn post_metadata_root(
    state: State<AppState>,
    base_url: BaseUrl,
    auth: crate::AuthContext,
    body: Json<tiled_core::schemas::PostMetadataRequest>,
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
    auth: crate::AuthContext,
    Json(req): Json<tiled_core::schemas::PostMetadataRequest>,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::WriteMetadata)?;
    auth.require(tiled_auth::Scope::CreateNode)?;
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
    auth: crate::AuthContext,
    req: tiled_core::schemas::PostMetadataRequest,
    generate_storage: bool,
) -> Result<impl IntoResponse, ServerError> {
    let path = segments.join("/");
    // Prefer the top-level `key` (Python tiled wire format, used by cirrus),
    // fall back to `metadata.key` for older clients, then synthesise.
    let id = req
        .key
        .clone()
        .or_else(|| {
            req.metadata
                .get("key")
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .unwrap_or_else(|| {
            let (nanos, counter) = synthetic_seed();
            format!("{:?}-{nanos:x}-{counter:x}", req.structure_family)
        });

    let structure_family = match req.structure_family {
        tiled_core::structures::StructureFamily::Container => "container",
        tiled_core::structures::StructureFamily::Array => "array",
        tiled_core::structures::StructureFamily::Table => "table",
        tiled_core::structures::StructureFamily::Sparse => "sparse",
        tiled_core::structures::StructureFamily::Awkward => "awkward",
        tiled_core::structures::StructureFamily::Ragged => "ragged",
    }
    .to_string();

    if let Some(ref catalog) = state.catalog {
        // Per-ancestor auth gate on the parent container path.
        let auth = if !segments.is_empty() {
            resolve_entry(&state, auth, &segments, tiled_auth::Scope::CreateNode).await?
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

        let node = catalog
            .create_node(
                parent_id,
                segments.clone(),
                tiled_catalog::node::RegisterRequest {
                    key: id.clone(),
                    structure_family: structure_family.clone(),
                    metadata: req.metadata.clone(),
                    specs: serde_json::to_value(&req.specs).unwrap_or_default(),
                    access_blob: creator_access_blob(auth.principal.as_deref()),
                },
            )
            .await
            .map_err(map_catalog_err)?;

        // Persist any data sources sent with the create request.
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
                && ds.management != tiled_core::data_source::Management::External;
            let (mimetype, assets) = if generate {
                managed_init_storage(catalog, ds, &segments, &node.key)?
            } else {
                if !generate_storage
                    && ds.management != tiled_core::data_source::Management::External
                {
                    for a in &ds.assets {
                        catalog
                            .validate_managed_data_uri(&a.data_uri)
                            .map_err(map_catalog_err)?;
                    }
                }
                let assets: Vec<tiled_catalog::data_source::AssetSpec> = ds
                    .assets
                    .iter()
                    .map(|a| tiled_catalog::data_source::AssetSpec {
                        data_uri: a.data_uri.clone(),
                        is_directory: a.is_directory,
                        parameter: a.parameter.clone().unwrap_or_else(|| "data_uri".into()),
                        num: a.num.map(|n| n as i32),
                    })
                    .collect();
                (ds.mimetype.clone().unwrap_or_default(), assets)
            };
            let structure_json = ds
                .structure
                .as_ref()
                .and_then(|s| serde_json::to_value(s).ok())
                .unwrap_or_default();
            let spec = tiled_catalog::data_source::DataSourceSpec {
                structure_family: ds_family_str(ds.structure_family).to_string(),
                structure: structure_json,
                mimetype,
                parameters: ds.parameters.clone(),
                management: format!("{:?}", ds.management).to_lowercase(),
                assets,
            };
            catalog
                .create_data_source(node.id, spec)
                .await
                .map_err(map_catalog_err)?;
        }

        let child_path = if path.is_empty() {
            node.key.clone()
        } else {
            format!("{path}/{}", node.key)
        };
        // Notify subscribers — both the parent (so a watcher of the
        // container hears about the new child) and the new node itself
        // (so any "watch this path" subscriber that connected first sees
        // the create event).
        state.streaming_bus.publish(
            &path,
            crate::streaming::UpdateKind::ChildCreated {
                key: node.key.clone(),
                structure_family: structure_family.clone(),
            },
        );
        let links = tiled_core::links::links_for_node(req.structure_family, &base_url, &child_path);
        let resp = tiled_core::schemas::PostMetadataResponse {
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
    let links = tiled_core::links::links_for_node(req.structure_family, &base_url, &child_path);
    let resp = tiled_core::schemas::PostMetadataResponse {
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
fn creator_access_blob(principal: Option<&tiled_auth::Principal>) -> serde_json::Value {
    match principal {
        Some(p) => serde_json::json!({"user": p.uuid}),
        None => serde_json::Value::Object(Default::default()),
    }
}

fn ds_family_str(f: tiled_core::structures::StructureFamily) -> &'static str {
    use tiled_core::structures::StructureFamily as SF;
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
    family: tiled_core::structures::StructureFamily,
) -> Result<&'static str, ServerError> {
    use tiled_core::structures::StructureFamily as SF;
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
        other => Err(ServerError::UnsupportedMediaType(format!(
            "no managed-write backend for {} nodes in this build \
             (array: application/x-zarr or application/x-npy; \
             table: application/x-parquet or text/csv)",
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
fn managed_init_storage(
    catalog: &tiled_catalog::Catalog,
    ds: &tiled_core::data_source::DataSource,
    parent_segments: &[String],
    key: &str,
) -> Result<(String, Vec<tiled_catalog::data_source::AssetSpec>), ServerError> {
    use tiled_core::data_source::Management;

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

    match mimetype.as_str() {
        "application/x-npy" | "application/x-numpy" | "npy" => {
            let structure = managed_array_structure(ds, &mimetype)?;
            let (_data_uri, assets) =
                tiled_adapters::init_storage_npy(writable_root, &path_parts, &structure)
                    .map_err(ServerError::from)?;
            Ok((mimetype, to_asset_specs(assets)))
        }
        "application/x-zarr" => {
            #[cfg(feature = "zarr-adapter")]
            {
                let structure = managed_array_structure(ds, &mimetype)?;
                let (_data_uri, assets) =
                    tiled_adapters::init_storage_zarr(writable_root, &path_parts, &structure)
                        .map_err(ServerError::from)?;
                Ok((mimetype, to_asset_specs(assets)))
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
                    tiled_adapters::init_storage_csv(writable_root, &path_parts, &structure)
                        .map_err(ServerError::from)?;
                Ok((mimetype, to_asset_specs(assets)))
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
                    tiled_adapters::init_storage_parquet(writable_root, &path_parts, &structure)
                        .map_err(ServerError::from)?;
                Ok((mimetype, to_asset_specs(assets)))
            }
            #[cfg(not(feature = "parquet-adapter"))]
            {
                Err(ServerError::UnsupportedMediaType(
                    "parquet support not built in".into(),
                ))
            }
        }
        other => Err(ServerError::UnsupportedMediaType(format!(
            "managed writes are not supported for mimetype {other} in this build \
             (supported: application/x-zarr, application/x-npy for array nodes; \
             application/x-parquet, text/csv for table nodes)"
        ))),
    }
}

/// Validate that a managed table-mimetype create carries a table structure and
/// return it. The table analog of [`managed_array_structure`].
#[cfg(any(feature = "csv-adapter", feature = "parquet-adapter"))]
fn managed_table_structure(
    ds: &tiled_core::data_source::DataSource,
    mimetype: &str,
) -> Result<tiled_core::structures::TableStructure, ServerError> {
    if ds.structure_family != tiled_core::structures::StructureFamily::Table {
        return Err(ServerError::UnsupportedMediaType(format!(
            "mimetype {mimetype} is only valid for table nodes, not {}",
            ds_family_str(ds.structure_family)
        )));
    }
    match &ds.structure {
        Some(tiled_core::structures::AnyStructure::Table(t)) => Ok(t.clone()),
        _ => Err(ServerError::Validation(
            "a managed table create requires a table structure (schema + columns)".into(),
        )),
    }
}

/// Validate that a managed array-mimetype create carries an array structure and
/// return it. Shared by every array `init_storage` arm so the family check and
/// the "needs shape + dtype" error stay identical across formats.
fn managed_array_structure(
    ds: &tiled_core::data_source::DataSource,
    mimetype: &str,
) -> Result<tiled_core::structures::ArrayStructure, ServerError> {
    if ds.structure_family != tiled_core::structures::StructureFamily::Array {
        return Err(ServerError::UnsupportedMediaType(format!(
            "mimetype {mimetype} is only valid for array nodes, not {}",
            ds_family_str(ds.structure_family)
        )));
    }
    match &ds.structure {
        Some(tiled_core::structures::AnyStructure::Array(a)) => Ok(a.clone()),
        _ => Err(ServerError::Validation(
            "a managed array create requires an array structure (shape + dtype)".into(),
        )),
    }
}

/// Map adapter-generated [`tiled_core::data_source::Asset`]s to the catalog's
/// `AssetSpec` persistence shape. Shared by every `init_storage` arm.
fn to_asset_specs(
    assets: Vec<tiled_core::data_source::Asset>,
) -> Vec<tiled_catalog::data_source::AssetSpec> {
    assets
        .into_iter()
        .map(|a| tiled_catalog::data_source::AssetSpec {
            data_uri: a.data_uri,
            is_directory: a.is_directory,
            parameter: a.parameter.unwrap_or_else(|| "data_uri".into()),
            num: a.num.map(|n| n as i32),
        })
        .collect()
}

fn map_catalog_err(e: tiled_catalog::CatalogError) -> ServerError {
    use tiled_catalog::CatalogError as CE;
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

/// Convert a catalog ORM DataSource row (+ its asset rows) into the
/// `tiled_core::data_source::DataSource` wire type used in API responses.
fn catalog_ds_to_core_ds(
    ds: tiled_catalog::orm::DataSource,
    assets: Vec<tiled_catalog::orm::Asset>,
) -> tiled_core::data_source::DataSource {
    let management = serde_json::from_value(serde_json::Value::String(ds.management.clone()))
        .unwrap_or(tiled_core::data_source::Management::Writable);
    let structure_family = ds
        .structure_family
        .parse::<tiled_core::structures::StructureFamily>()
        .unwrap_or(tiled_core::structures::StructureFamily::Container);
    let core_assets = assets
        .into_iter()
        .map(|a| tiled_core::data_source::Asset {
            data_uri: a.data_uri,
            is_directory: a.is_directory,
            parameter: if a.parameter.is_empty() {
                None
            } else {
                Some(a.parameter)
            },
            num: a.num.map(|n| n as usize),
            id: Some(a.id),
        })
        .collect();
    tiled_core::data_source::DataSource {
        structure_family,
        structure: serde_json::from_value::<Option<tiled_core::structures::AnyStructure>>(
            ds.structure,
        )
        .ok()
        .flatten(),
        id: Some(ds.id),
        mimetype: if ds.mimetype.is_empty() {
            None
        } else {
            Some(ds.mimetype)
        },
        parameters: ds.parameters,
        properties: serde_json::Value::Null,
        assets: core_assets,
        management,
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
    auth: crate::AuthContext,
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
    resolve_entry(&state, auth, &segments, tiled_auth::Scope::WriteMetadata).await?;
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

    // NOTE: the `access_blob` patch document and the policy.modify_node hook
    // are intentionally not applied — see the UNFIXED note in the worker
    // report. Python discards the access_blob patch whenever no access policy
    // exposes modify_node (router.py:2401-2404), which is exactly this crate's
    // configuration (tiled-access AccessPolicy has no modify_node), so the
    // stored access_blob is returned unchanged either way.
    let updated = catalog
        .update_metadata(node.id, metadata, specs, drop_revision)
        .await
        .map_err(map_catalog_err)?;
    let path = segments.join("/");
    state.streaming_bus.publish(
        &path,
        crate::streaming::UpdateKind::MetadataUpdated {
            metadata: updated.metadata.clone(),
            specs: updated.specs.clone(),
        },
    );
    let family = parse_structure_family(&updated.structure_family)?;
    let links = tiled_core::links::links_for_node(family, &base_url, &path);
    Ok(Json(tiled_core::schemas::PostMetadataResponse {
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
    auth: crate::AuthContext,
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
    resolve_entry(&state, auth, &segments, tiled_auth::Scope::WriteMetadata).await?;
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

    let updated = catalog
        .update_metadata(node.id, metadata, specs, drop_revision)
        .await
        .map_err(map_catalog_err)?;

    // access_blob: this crate has no access policy that exposes `modify_node`,
    // so the stored access_blob is never changed (Python's `else` branch,
    // router.py:2484-2487). `access_blob_modified` is true only when the client
    // SENT an access_blob differing from the (unchanged) stored one; the
    // response then echoes the unchanged value to signal the change was not
    // applied. `update_metadata` preserves access_blob, so `updated.access_blob`
    // is the current value.
    let access_blob_modified = req
        .get("access_blob")
        .filter(|v| !v.is_null())
        .map(|v| v != &updated.access_blob)
        .unwrap_or(false);

    let path = segments.join("/");
    state.streaming_bus.publish(
        &path,
        crate::streaming::UpdateKind::MetadataUpdated {
            metadata: updated.metadata.clone(),
            specs: updated.specs.clone(),
        },
    );

    // Response mirrors Python `json_or_msgpack(response_data)`: `{id}` plus
    // `access_blob` only when modified. Python includes `metadata` only when a
    // spec validator mutated it; this crate runs no spec validators, so metadata
    // is never modified here and is omitted. No `links` / `data_sources` (the
    // Python handler's response dict sets neither).
    Ok(Json(tiled_core::schemas::PostMetadataResponse {
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
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::ReadMetadata)?;
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
    resolve_entry(&state, auth, &segments, tiled_auth::Scope::ReadMetadata).await?;
    let node = catalog
        .lookup(&segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;

    let revisions = catalog
        .list_revisions(node.id, offset, limit)
        .await
        .map_err(map_catalog_err)?;
    let count = revisions.len();
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
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::DeleteRevision)?;
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
    resolve_entry(&state, auth, &segments, tiled_auth::Scope::DeleteRevision).await?;
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
/// `path_from_uri`, utils.py:745). `url::Url::to_file_path` handles the
/// `file://host/path` and `file:///path` forms plus percent-decoding.
fn path_from_file_uri(uri: &str) -> Result<std::path::PathBuf, ServerError> {
    // The `file:` prefix is validated by the caller before this runs, so a
    // failure here means the stored data_uri is corrupt — a server-side data
    // integrity problem, not bad client input. Classify as Internal (500) so
    // the URI is logged server-side and not echoed to the client.
    let url = url::Url::parse(uri)
        .map_err(|e| ServerError::Internal(format!("invalid asset data_uri '{uri}': {e}")))?;
    url.to_file_path()
        .map_err(|_| ServerError::Internal(format!("asset data_uri is not a file path: {uri}")))
}

/// Shared prelude for both asset endpoints, in Python's check order
/// (router.py:2584-2615): resolve + scope-gate the entry, apply the
/// `expose_raw_assets` policy (403), require a catalog (== Python's
/// `hasattr(entry, "asset_by_id")` → 405 for the in-memory tree), look up the
/// node, then fetch the node-scoped asset (404 if absent).
async fn resolve_asset(
    state: &AppState,
    auth: crate::AuthContext,
    segments: &[String],
    asset_id: i64,
) -> Result<tiled_catalog::orm::Asset, ServerError> {
    // Per-ancestor auth gate + terminal read:data (Python get_entry
    // scopes=["read:data"]). resolve_entry narrows per node and enforces
    // read:data at the resolved entry. Bad path → 404, insufficient scope → 403.
    resolve_entry(state, auth, segments, tiled_auth::Scope::ReadData).await?;
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
    auth: crate::AuthContext,
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
        if std::path::Path::new(rel).is_absolute() {
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
    auth: crate::AuthContext,
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
    auth: crate::AuthContext,
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
    // Per-ancestor auth gate (compound scope: WriteData or WriteMetadata).
    let auth = resolve_entry_catalog(&state, auth, &segments).await?;
    auth.require(tiled_auth::Scope::WriteData)
        .or_else(|_| auth.require(tiled_auth::Scope::WriteMetadata))?;
    let node = catalog
        .lookup(&segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;
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
    let updated = catalog
        .update_data_source(id, structure, parameters)
        .await
        .map_err(map_catalog_err)?;
    // Notify subscribers — a new partition / chunk likely became
    // available. tiled#1339 made the Python server emit DataAppended on
    // the same path for the same reason.
    let path = segments.join("/");
    state.streaming_bus.publish(
        &path,
        crate::streaming::UpdateKind::DataAppended { partition: None },
    );
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
    auth: crate::AuthContext,
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
    let catalog = state.catalog.as_ref().ok_or_else(|| {
        ServerError::Validation("server has no catalog DB; DELETE not supported".into())
    })?;
    if segments.is_empty() {
        return Err(ServerError::Validation(
            "cannot DELETE the catalog root".into(),
        ));
    }
    // Per-ancestor auth gate: narrows at every prefix and requires
    // DeleteNode on the narrowed set — same invariant as the read gate.
    resolve_entry(&state, auth, &segments, tiled_auth::Scope::DeleteNode).await?;
    let node = catalog
        .lookup(&segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;
    // Reject deletion of a non-empty container (upstream tiled #503).
    // Cascading FK delete *would* succeed, but silently dropping a
    // subtree is the kind of thing that needs explicit `rm -rf`
    // semantics; require the caller to empty the container first.
    if node.structure_family == "container" {
        let kid_count = catalog
            .count_children(Some(node.id))
            .await
            .map_err(map_catalog_err)?;
        if kid_count > 0 {
            // 409 Conflict, matching Python's Conflicts handler
            // (adapter.py:1024 -> app.py:350-353).
            return Err(ServerError::Conflict(format!(
                "container '{}' is not empty ({kid_count} children); \
                 delete its contents first or use a future recursive endpoint",
                segments.join("/"),
            )));
        }
    }
    catalog
        .delete_node(node.id, external_only)
        .await
        .map_err(map_catalog_err)?;
    let path = segments.join("/");
    state
        .streaming_bus
        .publish(&path, crate::streaming::UpdateKind::NodeDeleted);
    Ok(axum::http::StatusCode::NO_CONTENT)
}

/// Build a `Resource` for the catalog by reading the DB directly. Skips
/// the `CatalogAdapter`'s in-memory cache so a same-request read after a
/// write sees the latest state.
async fn catalog_metadata_resource(
    catalog: &tiled_catalog::Catalog,
    segments: &[String],
    base_url: &str,
    include_data_sources: bool,
) -> Result<tiled_core::schemas::Resource, ServerError> {
    use tiled_core::schemas::{
        NodeAttributes, NodeStructure, Resource, SortDirection, SortingItem,
    };
    if segments.is_empty() {
        let count = catalog
            .count_children(None)
            .await
            .map_err(map_catalog_err)?;
        let links = tiled_core::links::links_for_node(
            tiled_core::structures::StructureFamily::Container,
            base_url,
            "",
        );
        return Ok(Resource {
            id: String::new(),
            attributes: NodeAttributes {
                ancestors: vec![],
                structure_family: Some(tiled_core::structures::StructureFamily::Container),
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

    let node = catalog
        .lookup(segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;
    let path = segments.join("/");
    let id = segments.last().cloned().unwrap_or_default();
    let family = parse_structure_family(&node.structure_family)?;
    let links = tiled_core::links::links_for_node(family, base_url, &path);
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
        if matches!(family, tiled_core::structures::StructureFamily::Container) {
            let count = catalog
                .count_children(Some(node.id))
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
                    result.push(catalog_ds_to_core_ds(ds, asset_rows));
                }
                Some(result)
            } else {
                None
            };
            (sv, ds_list)
        };
    let sorting = if matches!(family, tiled_core::structures::StructureFamily::Container) {
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

fn parse_structure_family(s: &str) -> Result<tiled_core::structures::StructureFamily, ServerError> {
    use tiled_core::structures::StructureFamily as SF;
    match s {
        "container" => Ok(SF::Container),
        "array" => Ok(SF::Array),
        "table" => Ok(SF::Table),
        "sparse" => Ok(SF::Sparse),
        "awkward" => Ok(SF::Awkward),
        other => Err(ServerError::Validation(format!(
            "unknown structure_family in DB: {other}"
        ))),
    }
}

/// Distinct (wall-clock, counter) seed used to synthesise IDs when the
/// caller didn't supply a `key`. The two values are kept separate (not
/// XORed) so concurrent POSTs in the same nanosecond can't collide via
/// any combination of (nanos, counter) values.
fn synthetic_seed() -> (u64, u64) {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    (nanos, COUNTER.fetch_add(1, Ordering::Relaxed))
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
    use tiled_core::dtype::{BuiltinDType, DynNDArray, Endianness, Kind};

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
}
