//! Route handlers for the Tiled API.

use std::collections::HashMap;

use axum::Json;
use axum::extract::{OriginalUri, Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use serde::Deserialize;

use crate::extractors::PathSegments;

use tiled_core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_core::links;
use tiled_core::schemas::{About, AboutAuthentication, Response};

use crate::core;
use crate::error::ServerError;
use crate::extractors::BaseUrl;
use crate::state::AppState;

/// Helper that turns axum's [`OriginalUri`] into a list of percent-decoded
/// path segments after stripping the API prefix.
fn segments_from_uri(uri: &axum::http::Uri, prefix: &str) -> Vec<String> {
    PathSegments::from_raw_path(uri.path(), prefix).0
}

/// Run `walk_tree` on the blocking pool for its side effect of warming any
/// lazy adapter caches (e.g. `tiled_mongo::MongoCatalog::load_runs`). The
/// caller can then do the real walk synchronously in async context — by
/// then `OnceLock`-cached children are present and `get()` is O(1).
///
/// Returns the same path-not-found error the caller would see from a
/// direct walk, so handlers can `?` it before re-walking.
async fn pre_warm_walk(state: &AppState, segments: &[String]) -> Result<(), ServerError> {
    let state = state.clone();
    let segments = segments.to_vec();
    tokio::task::spawn_blocking(move || -> Result<(), ServerError> {
        let _ = core::walk_tree(state.root_tree.as_ref(), &segments)?;
        Ok(())
    })
    .await
    .map_err(|e| ServerError::Internal(format!("blocking walk: {e}")))?
}

// ---------------------------------------------------------------------------
// Operational endpoints
// ---------------------------------------------------------------------------

pub async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

pub async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    // Use the blocking pool — for adapters like tiled_mongo's MongoCatalog,
    // the first `.len()` may trigger a sync DB load.
    let count = tokio::task::spawn_blocking(move || state.root_tree.len())
        .await
        .unwrap_or(0);
    Json(serde_json::json!({"status": "ok", "nodes": count}))
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
    let auth_required =
        !providers.is_empty() || state.external_oidc.is_some();

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
        meta: HashMap::from([("root_path".into(), serde_json::Value::String(String::new()))]),
    };

    Json(about)
}

// ---------------------------------------------------------------------------
// GET /api/v1/metadata/{*path}
// ---------------------------------------------------------------------------

pub async fn metadata_root(
    state: State<AppState>,
    base_url: BaseUrl,
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    metadata(
        state,
        OriginalUri("/api/v1/metadata/".parse().expect("static URI")),
        base_url,
        auth,
    )
    .await
}

pub async fn metadata(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    BaseUrl(base_url): BaseUrl,
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::ReadMetadata)?;
    // Use the raw URI path so a key containing `%2F` survives as one
    // segment rather than being split apart by axum's `Path<String>` (which
    // percent-decodes before splitting).
    let segments = segments_from_uri(&uri, "/api/v1/metadata/");
    // When a SQL catalog is wired, read directly through it: the
    // CatalogAdapter caches children eagerly to satisfy the sync trait,
    // and PATCH/DELETE write past that cache, so a same-request read after
    // a write would otherwise see stale data. Direct DB lookup keeps
    // metadata responses consistent with the latest committed write.
    let resource = if let Some(ref catalog) = state.catalog {
        catalog_metadata_resource(catalog, &segments, &base_url).await?
    } else {
        // The tree walk + Resource construction may invoke blocking
        // adapters (e.g. `MongoCatalog::get` triggers a sync MongoDB
        // query the first time). Run them on the blocking thread pool so
        // async workers stay responsive.
        tokio::task::spawn_blocking(move || -> Result<_, ServerError> {
            if segments.is_empty() {
                Ok(core::construct_root_resource(
                    state.root_tree.as_ref(),
                    &base_url,
                ))
            } else {
                let adapter = core::walk_tree(state.root_tree.as_ref(), &segments)?;
                let id = segments.last().cloned().unwrap_or_default();
                let path = segments.join("/");
                Ok(core::construct_resource(adapter, &id, &path, &base_url))
            }
        })
        .await
        .map_err(|e| ServerError::Internal(format!("blocking task failed: {e}")))??
    };

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

pub async fn search_root(
    state: State<AppState>,
    params: Query<HashMap<String, String>>,
    base_url: BaseUrl,
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    search(
        state,
        OriginalUri("/api/v1/search/".parse().expect("static URI")),
        params,
        base_url,
        auth,
    )
    .await
}

pub async fn search(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    BaseUrl(base_url): BaseUrl,
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::ReadMetadata)?;
    let segments = segments_from_uri(&uri, "/api/v1/search/");

    let offset: usize = params
        .get("page[offset]")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let limit: usize = params
        .get("page[limit]")
        .and_then(|v| v.parse().ok())
        .unwrap_or(links::DEFAULT_PAGE_SIZE)
        .min(links::MAX_PAGE_SIZE);

    let filter_params: Vec<(String, String)> = params
        .iter()
        .filter(|(k, _)| k.starts_with("filter["))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let queries = tiled_core::queries::decode_query_filters(&filter_params);

    if let Some(ref catalog) = state.catalog {
        // Push filters down to SQL — avoids materialising every child in
        // memory, and (more importantly) avoids the CatalogAdapter cache
        // which would otherwise return stale rows after a write.
        let parent_id = if segments.is_empty() {
            None
        } else {
            let parent = catalog
                .lookup(&segments)
                .await
                .map_err(map_catalog_err)?
                .ok_or_else(|| {
                    ServerError::NotFound(format!("'{}' not found", segments.join("/")))
                })?;
            if parent.structure_family != "container" {
                return Err(ServerError::Validation(format!(
                    "'{}' is not a container",
                    segments.join("/")
                )));
            }
            Some(parent.id)
        };

        let (rows, total) = catalog
            .search_children(parent_id, &queries, offset as i64, limit as i64)
            .await
            .map_err(map_catalog_err)?;
        let logical_path = segments.join("/");
        let path_trimmed = logical_path.trim_matches('/');
        let entries: Vec<tiled_core::schemas::Resource> = rows
            .into_iter()
            .map(|node| {
                let family = parse_structure_family(&node.structure_family).unwrap_or(
                    tiled_core::structures::StructureFamily::Container,
                );
                let child_path = if path_trimmed.is_empty() {
                    node.key.clone()
                } else {
                    format!("{path_trimmed}/{}", node.key)
                };
                let links = tiled_core::links::links_for_node(family, &base_url, &child_path);
                tiled_core::schemas::Resource {
                    id: node.key,
                    attributes: tiled_core::schemas::NodeAttributes {
                        ancestors: node.ancestors,
                        structure_family: Some(family),
                        specs: serde_json::from_value(node.specs).unwrap_or_default(),
                        metadata: Some(node.metadata),
                        structure: None,
                        access_blob: Some(node.access_blob),
                        sorting: None,
                        data_sources: None,
                    },
                    links,
                }
            })
            .collect();
        let pagination = tiled_core::links::pagination_links(
            &base_url,
            "search",
            &logical_path,
            offset,
            limit,
            total as usize,
        );
        return Ok(Json(tiled_core::schemas::Response {
            data: Some(entries),
            error: None,
            links: Some(serde_json::to_value(&pagination).unwrap_or_default()),
            meta: Some(
                serde_json::to_value(tiled_core::schemas::ContainerMeta {
                    count: total as usize,
                })
                .unwrap_or_default(),
            ),
        }));
    }

    // Walk + paginate on the blocking pool: container.search() / .get() may
    // call into MongoDB.
    let resp = tokio::task::spawn_blocking(move || -> Result<_, ServerError> {
        let container: &dyn ContainerAdapter = if segments.is_empty() {
            state.root_tree.as_ref()
        } else {
            let adapter = core::walk_tree(state.root_tree.as_ref(), &segments)?;
            match adapter {
                AnyAdapter::Container(c) => c.as_ref(),
                _ => {
                    return Err(ServerError::Validation(format!(
                        "'{}' is not a container",
                        segments.join("/")
                    )));
                }
            }
        };
        let logical_path = segments.join("/");
        Ok(core::construct_entries_response(
            container,
            &logical_path,
            &base_url,
            offset,
            limit,
            &queries,
        ))
    })
    .await
    .map_err(|e| ServerError::Internal(format!("blocking task failed: {e}")))??;
    Ok(Json(resp))
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
    pre_warm_walk(&state, &segments).await?;

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments)?;
    let array_adapter = match adapter {
        AnyAdapter::Array(a) => a.as_ref(),
        _ => {
            return Err(ServerError::Validation(format!(
                "'{}' is not an array",
                segments.join("/")
            )));
        }
    };

    let block_str = params.get("block").map(|s| s.as_str()).unwrap_or("");
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
    let slice = match params.get("slice").map(|s| s.as_str()) {
        None | Some("") => tiled_core::ndslice::NDSlice::empty(),
        Some(s) => tiled_core::ndslice::NDSlice::from_numpy_str(s)
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
        // multi-chunk block requires applying the slice to the
        // assembled buffer; restrict to "no slice" for the first port
        // and surface a clear 422 if both are combined. Slice across a
        // chunk range is a follow-up (needs a contiguous-buffer
        // apply_slice helper).
        if !slice.is_empty() {
            return Err(ServerError::Validation(
                "?slice= combined with a multi-chunk ?block= range is not yet supported".into(),
            ));
        }
        read_block_range(array_adapter, &block_specs).await?
    };

    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream");

    let media_type = tiled_serialization::resolve_media_type(
        accept,
        tiled_core::structures::StructureFamily::Array,
        &state.serialization_registry,
    )
    .unwrap_or_else(|| "application/octet-stream".to_string());

    let body = if let Some(serializer) = state
        .serialization_registry
        .dispatch(tiled_core::structures::StructureFamily::Array, &media_type)
    {
        let ser_meta = serde_json::json!({
            "itemsize": data.dtype.element_size(),
            "kind": String::from(data.dtype.kind.to_numpy_char()),
            "shape": data.shape,
        });
        serializer(&data.data, &ser_meta).map_err(|e| ServerError::Internal(e.to_string()))?
    } else {
        data.data
    };

    Ok(serve_with_range(&headers, &media_type, body))
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
                    (header::CONTENT_TYPE, HeaderValue::from_str(content_type)
                        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"))),
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
                (header::CONTENT_TYPE, HeaderValue::from_str(content_type)
                    .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"))),
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
        if total == 0 { return None; }
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
    container_full(state, OriginalUri(uri), BaseUrl(base_url), headers, auth)
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

pub async fn container_full(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    BaseUrl(base_url): BaseUrl,
    headers: HeaderMap,
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/container/full/");
    pre_warm_walk(&state, &segments).await?;

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments)?;
    let container = adapter
        .as_container()
        .ok_or_else(|| ServerError::Validation(format!(
            "'{}' is not a container",
            segments.join("/")
        )))?;

    // Build the same Vec<Resource> shape /search emits — that's what
    // the registered container serializers (html, json-seq) consume.
    let path = segments.join("/");
    let children: Vec<tiled_core::schemas::Resource> = container
        .keys()
        .iter()
        .filter_map(|k| container.get(k).map(|child| {
            let child_path = if path.is_empty() {
                k.clone()
            } else {
                format!("{path}/{k}")
            };
            core::construct_resource(child, k, &child_path, &base_url)
        }))
        .collect();
    let body_json =
        serde_json::to_vec(&children).map_err(|e| ServerError::Internal(format!("encode: {e}")))?;

    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");
    let media_type = tiled_serialization::resolve_media_type(
        accept,
        tiled_core::structures::StructureFamily::Container,
        &state.serialization_registry,
    )
    .unwrap_or_else(|| "text/html".to_string());

    let body = if let Some(serializer) = state
        .serialization_registry
        .dispatch(tiled_core::structures::StructureFamily::Container, &media_type)
    {
        let meta = serde_json::json!({"path": path});
        serializer(&body_json, &meta).map_err(|e| ServerError::Internal(e.to_string()))?
    } else {
        return Err(ServerError::Validation(format!(
            "no container serializer for {media_type}"
        )));
    };

    Ok(serve_with_range(&headers, &media_type, body))
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
            return Err(ServerError::Validation(format!(
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
    if ndim == 0 || chunk_shape.iter().any(|d| *d == 0) {
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
// For single-block adapters this is the natural read path. Multi-chunk
// adapters currently return only block 0,0,...,0 — concat across blocks
// is a follow-up. Mirrors upstream tiled's `/array/full/` endpoint, which
// the SPA uses via `links.full`.
pub async fn array_full(
    state: State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(mut params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    // Translate /array/full/<p> → /array/block/<p> with implicit
    // block=0,0,...,0. The block handler already does the right thing
    // when the param is absent.
    let path = uri.path().replacen("/api/v1/array/full/", "/api/v1/array/block/", 1);
    let new_uri: axum::http::Uri = path.parse().map_err(|e| {
        ServerError::Internal(format!("malformed /array/full/ URI: {e}"))
    })?;
    params.remove("block");
    array_block(state, OriginalUri(new_uri), Query(params), headers, auth).await
}

// ---------------------------------------------------------------------------
// GET /api/v1/table/partition/{*path}
// ---------------------------------------------------------------------------

pub async fn table_partition(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::ReadData)?;
    let segments = segments_from_uri(&uri, "/api/v1/table/partition/");
    pre_warm_walk(&state, &segments).await?;

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments)?;
    let table_adapter = match adapter {
        AnyAdapter::Table(t) => t.as_ref(),
        _ => {
            return Err(ServerError::Validation(format!(
                "'{}' is not a table",
                segments.join("/")
            )));
        }
    };

    let partition: usize = params
        .get("partition")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let fields: Option<Vec<String>> = params
        .get("field")
        .map(|f| f.split(',').map(|s| s.trim().to_string()).collect());

    let table = table_adapter
        .read_partition(partition, fields.as_deref())
        .await
        .map_err(ServerError::from)?;

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

    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "application/vnd.apache.arrow.file".to_string(),
        )],
        buf,
    )
        .into_response())
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
    pre_warm_walk(&state, &segments).await?;

    let adapter = core::walk_tree(state.root_tree.as_ref(), &segments)?;

    // The run must be a container (BlueskyRun).
    let run = adapter
        .as_container()
        .ok_or_else(|| ServerError::Validation("This is not a BlueskyRun".into()))?;

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
    for stream_key in run.keys() {
        if let Some(AnyAdapter::Container(stream)) = run.get(&stream_key) {
            let stream_meta = stream.metadata();
            if let Some(descriptors) = stream_meta.get("descriptors") {
                if let Some(arr) = descriptors.as_array() {
                    for desc in arr {
                        let line = serde_json::json!({"name": "descriptor", "doc": desc});
                        lines.push(serde_json::to_string(&line).unwrap_or_default());
                    }
                }
            }
        }
    }

    // Emit stop document.
    if let Some(stop) = meta.get("stop") {
        if !stop.is_null() {
            let line = serde_json::json!({"name": "stop", "doc": stop});
            lines.push(serde_json::to_string(&line).unwrap_or_default());
        }
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
    auth.require(tiled_auth::Scope::Register)
        .or_else(|_| auth.require(tiled_auth::Scope::WriteMetadata))?;
    let segments = segments_from_uri(&uri, "/api/v1/register/");
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
    }
    .to_string();

    if let Some(ref catalog) = state.catalog {
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
                    access_blob: serde_json::Value::Object(Default::default()),
                },
            )
            .await
            .map_err(map_catalog_err)?;

        // Persist any data sources sent with the create request.
        for ds in &req.data_sources {
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
            let structure_json = ds
                .structure
                .as_ref()
                .and_then(|s| serde_json::to_value(s).ok())
                .unwrap_or_default();
            let spec = tiled_catalog::data_source::DataSourceSpec {
                structure_family: ds_family_str(ds.structure_family).to_string(),
                structure: structure_json,
                mimetype: ds.mimetype.clone().unwrap_or_default(),
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
        state
            .streaming_bus
            .publish(
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

fn ds_family_str(f: tiled_core::structures::StructureFamily) -> &'static str {
    use tiled_core::structures::StructureFamily as SF;
    match f {
        SF::Container => "container",
        SF::Array => "array",
        SF::Table => "table",
        SF::Sparse => "sparse",
        SF::Awkward => "awkward",
    }
}

fn map_catalog_err(e: tiled_catalog::CatalogError) -> ServerError {
    use tiled_catalog::CatalogError as CE;
    match e {
        CE::NotFound(m) => ServerError::NotFound(m),
        CE::Validation(m) => ServerError::Validation(m),
        CE::Conflict(m) => ServerError::Validation(m),
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
// JSON body: `{ "metadata": {...}, "specs": [...] }`. Everything else
// (links, data_sources) is read-only here — use PUT /data_source for
// structural changes.

pub async fn patch_metadata(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    BaseUrl(base_url): BaseUrl,
    headers: HeaderMap,
    auth: crate::AuthContext,
    Json(req): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::WriteMetadata)?;
    // Optional `?drop_revision=true` (upstream tiled #972). When set,
    // the previous (metadata, specs, access_blob) is discarded instead
    // of pushed onto the revisions table — useful for high-frequency
    // updates where the revision history would dominate storage.
    let drop_revision = params
        .get("drop_revision")
        .map(|v| matches!(v.as_str(), "true" | "True" | "1" | "yes"))
        .unwrap_or(false);
    let segments = segments_from_uri(&uri, "/api/v1/metadata/");
    let catalog = state
        .catalog
        .as_ref()
        .ok_or_else(|| ServerError::Validation("server has no catalog DB; PATCH not supported".into()))?;
    if segments.is_empty() {
        return Err(ServerError::Validation("cannot PATCH the catalog root".into()));
    }
    let node = catalog
        .lookup(&segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;
    // Per-node AccessPolicy decision (tiled#287). When a policy is
    // wired, this narrows the session's scopes by what the policy says
    // about THIS node (e.g. tag-based). The require() check that
    // follows uses the narrowed set, so policy denials surface as 403.
    let auth = auth
        .narrow_for_node(
            state.access_policy.as_deref(),
            tiled_access::NodeContext {
                path: &segments,
                structure_family: &node.structure_family,
                metadata: &node.metadata,
                access_blob: &node.access_blob,
            },
        )
        .await;
    auth.require(tiled_auth::Scope::WriteMetadata)?;

    // Content-Type-driven dispatch (upstream tiled #688):
    //
    //   * application/json-patch+json   — body is an array of RFC 6902 ops
    //     applied to the existing metadata + specs in place;
    //   * application/merge-patch+json  — body is a partial document merged
    //     into existing metadata + specs per RFC 7396 (null fields delete);
    //   * default (any other / missing) — historical "partial replace": top-
    //     level `metadata` and/or `specs` keys overwrite the old values.
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let mode = if content_type.contains("application/json-patch+json") {
        PatchMode::JsonPatch
    } else if content_type.contains("application/merge-patch+json") {
        PatchMode::MergePatch
    } else {
        PatchMode::PartialReplace
    };
    let (metadata, specs) = match mode {
        PatchMode::PartialReplace => {
            let metadata = req
                .get("metadata")
                .cloned()
                .unwrap_or_else(|| node.metadata.clone());
            let specs = req
                .get("specs")
                .cloned()
                .unwrap_or_else(|| node.specs.clone());
            (metadata, specs)
        }
        PatchMode::JsonPatch => {
            let ops_array = req.as_array().ok_or_else(|| {
                ServerError::Validation(
                    "application/json-patch+json body must be a JSON array of ops".into(),
                )
            })?;
            // Build a single working doc {metadata, specs}, run the ops,
            // then split it back. RFC 6902 paths starting with /metadata
            // or /specs work without further translation.
            let mut working = serde_json::json!({
                "metadata": node.metadata,
                "specs": node.specs,
            });
            let patch: json_patch::Patch = serde_json::from_value(
                serde_json::Value::Array(ops_array.clone()),
            )
            .map_err(|e| {
                ServerError::Validation(format!("invalid json-patch: {e}"))
            })?;
            json_patch::patch(&mut working, &patch).map_err(|e| {
                ServerError::Validation(format!("json-patch failed: {e}"))
            })?;
            let metadata = working
                .get("metadata")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let specs = working
                .get("specs")
                .cloned()
                .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
            (metadata, specs)
        }
        PatchMode::MergePatch => {
            // RFC 7396: recursively merge into the working doc; null
            // values in the patch delete the corresponding key. We only
            // merge the top-level `metadata` and `specs` fields (everything
            // else on the body is ignored — this matches upstream's
            // behaviour where merge-patch doesn't touch structure).
            let mut metadata = node.metadata.clone();
            let mut specs = node.specs.clone();
            if let Some(m) = req.get("metadata") {
                merge_patch_apply(&mut metadata, m);
            }
            if let Some(s) = req.get("specs") {
                // specs is conventionally an array; replace wholesale on
                // merge-patch (RFC 7396 says non-objects are replaced).
                specs = s.clone();
            }
            (metadata, specs)
        }
    };
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
    auth.require(tiled_auth::Scope::WriteData)
        .or_else(|_| auth.require(tiled_auth::Scope::WriteMetadata))?;
    let segments = segments_from_uri(&uri, "/api/v1/data_source/");
    let catalog = state
        .catalog
        .as_ref()
        .ok_or_else(|| ServerError::Validation("server has no catalog DB; PUT not supported".into()))?;
    if segments.is_empty() {
        return Err(ServerError::Validation("PUT /data_source requires a node path".into()));
    }
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
    auth: crate::AuthContext,
) -> Result<impl IntoResponse, ServerError> {
    auth.require(tiled_auth::Scope::DeleteNode)?;
    let segments = segments_from_uri(&uri, "/api/v1/metadata/");
    let catalog = state
        .catalog
        .as_ref()
        .ok_or_else(|| ServerError::Validation("server has no catalog DB; DELETE not supported".into()))?;
    if segments.is_empty() {
        return Err(ServerError::Validation("cannot DELETE the catalog root".into()));
    }
    let node = catalog
        .lookup(&segments)
        .await
        .map_err(map_catalog_err)?
        .ok_or_else(|| ServerError::NotFound(format!("'{}' not found", segments.join("/"))))?;
    let auth = auth
        .narrow_for_node(
            state.access_policy.as_deref(),
            tiled_access::NodeContext {
                path: &segments,
                structure_family: &node.structure_family,
                metadata: &node.metadata,
                access_blob: &node.access_blob,
            },
        )
        .await;
    auth.require(tiled_auth::Scope::DeleteNode)?;
    catalog.delete_node(node.id).await.map_err(map_catalog_err)?;
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
) -> Result<tiled_core::schemas::Resource, ServerError> {
    use tiled_core::schemas::{NodeAttributes, NodeStructure, Resource, SortingItem, SortDirection};
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
    // For container nodes, surface the child count so the client doesn't
    // need an extra `/search/` round-trip. Leaves carry their data-source
    // structure when present.
    let structure_value = if matches!(family, tiled_core::structures::StructureFamily::Container) {
        let count = catalog
            .count_children(Some(node.id))
            .await
            .map_err(map_catalog_err)?;
        Some(
            serde_json::to_value(&NodeStructure {
                contents: None,
                count: count as usize,
            })
            .unwrap_or_default(),
        )
    } else {
        catalog
            .list_data_sources(node.id)
            .await
            .map_err(map_catalog_err)?
            .first()
            .map(|ds| ds.structure.clone())
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
            data_sources: None,
        },
        links,
    })
}

fn parse_structure_family(
    s: &str,
) -> Result<tiled_core::structures::StructureFamily, ServerError> {
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
        let expected = vec![
            0, 0, 0, 0,
            0, b'a', b'b', 0,
            0, b'c', b'd', 0,
            0, 0, 0, 0,
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn copy_chunk_2d_multi_byte() {
        // 2x2 result, chunk is 1x2 placed at (1, 0). element_size=2
        // Each 2-byte element is little-endian u16; values 100, 200
        let mut result = vec![0u8; 8];
        let chunk = (100u16).to_le_bytes().iter()
            .chain((200u16).to_le_bytes().iter())
            .copied().collect::<Vec<_>>();
        copy_chunk_into_result(&mut result, &[2, 2], &[1, 0], &chunk, &[1, 2], 2);
        // Bytes 4..6 = 100, bytes 6..8 = 200.
        assert_eq!(&result[4..6], &(100u16).to_le_bytes());
        assert_eq!(&result[6..8], &(200u16).to_le_bytes());
    }
}

/// Which dispatch arm `patch_metadata` takes, derived from
/// `Content-Type`. Mirrors upstream tiled PR #688's three modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PatchMode {
    /// Default. Top-level `metadata`/`specs` keys overwrite the old values.
    PartialReplace,
    /// RFC 6902 ops array (`application/json-patch+json`).
    JsonPatch,
    /// RFC 7396 partial doc (`application/merge-patch+json`).
    MergePatch,
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
