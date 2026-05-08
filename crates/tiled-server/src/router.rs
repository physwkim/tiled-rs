//! Route handlers for the Tiled API.

use std::collections::HashMap;

use axum::Json;
use axum::extract::{OriginalUri, Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;

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

    let about = About {
        api_version: 0,
        library_version: env!("CARGO_PKG_VERSION").to_string(),
        formats,
        aliases,
        queries: state.query_names.clone(),
        authentication: AboutAuthentication {
            required: false,
            providers: vec![],
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
) -> Result<impl IntoResponse, ServerError> {
    metadata(
        state,
        OriginalUri("/api/v1/metadata/".parse().expect("static URI")),
        base_url,
    )
    .await
}

pub async fn metadata(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    BaseUrl(base_url): BaseUrl,
) -> Result<impl IntoResponse, ServerError> {
    // Use the raw URI path so a key containing `%2F` survives as one
    // segment rather than being split apart by axum's `Path<String>` (which
    // percent-decodes before splitting).
    let segments = segments_from_uri(&uri, "/api/v1/metadata/");
    // The tree walk + Resource construction may invoke blocking adapters
    // (e.g. `MongoCatalog::get` triggers a sync MongoDB query the first
    // time). Run them on the blocking thread pool so async workers stay
    // responsive.
    let resource = tokio::task::spawn_blocking(move || -> Result<_, ServerError> {
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
    .map_err(|e| ServerError::Internal(format!("blocking task failed: {e}")))??;

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
) -> Result<impl IntoResponse, ServerError> {
    search(
        state,
        OriginalUri("/api/v1/search/".parse().expect("static URI")),
        params,
        base_url,
    )
    .await
}

pub async fn search(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    BaseUrl(base_url): BaseUrl,
) -> Result<impl IntoResponse, ServerError> {
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
) -> Result<impl IntoResponse, ServerError> {
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
    let block: Vec<usize> = if block_str.is_empty() {
        vec![0; array_adapter.structure().ndim()]
    } else {
        block_str
            .split(',')
            .map(|s| {
                s.trim()
                    .parse::<usize>()
                    .map_err(|_| ServerError::Validation(format!("Invalid block index: {s}")))
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    // Honor the `slice` query parameter (numpy-style). Empty / missing →
    // full block.
    let slice = match params.get("slice").map(|s| s.as_str()) {
        None | Some("") => tiled_core::ndslice::NDSlice::empty(),
        Some(s) => tiled_core::ndslice::NDSlice::from_numpy_str(s)
            .map_err(|e| ServerError::Validation(format!("Invalid slice '{s}': {e}")))?,
    };
    let data = array_adapter
        .read_block(&block, &slice)
        .await
        .map_err(ServerError::from)?;

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

    Ok(([(axum::http::header::CONTENT_TYPE, media_type)], body).into_response())
}

// ---------------------------------------------------------------------------
// GET /api/v1/table/partition/{*path}
// ---------------------------------------------------------------------------

pub async fn table_partition(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, ServerError> {
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
) -> Result<impl IntoResponse, ServerError> {
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
    body: Json<tiled_core::schemas::PostMetadataRequest>,
) -> Result<impl IntoResponse, ServerError> {
    register(
        state,
        OriginalUri("/api/v1/register/".parse().expect("static URI")),
        base_url,
        body,
    )
    .await
}

pub async fn register(
    State(_state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    BaseUrl(base_url): BaseUrl,
    Json(req): Json<tiled_core::schemas::PostMetadataRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let segments = segments_from_uri(&uri, "/api/v1/register/");
    let path = segments.join("/");
    // Generate a synthetic ID from the request payload's structure_family +
    // a hash of the metadata. Real implementations would allocate via the
    // catalog database.
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
