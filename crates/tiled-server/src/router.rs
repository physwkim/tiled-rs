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
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    BaseUrl(base_url): BaseUrl,
    Json(req): Json<tiled_core::schemas::PostMetadataRequest>,
) -> Result<impl IntoResponse, ServerError> {
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
    BaseUrl(base_url): BaseUrl,
    Json(req): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ServerError> {
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
    let metadata = req
        .get("metadata")
        .cloned()
        .unwrap_or_else(|| node.metadata.clone());
    let specs = req
        .get("specs")
        .cloned()
        .unwrap_or_else(|| node.specs.clone());
    let updated = catalog
        .update_metadata(node.id, metadata, specs)
        .await
        .map_err(map_catalog_err)?;
    let path = segments.join("/");
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
    Json(req): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ServerError> {
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
) -> Result<impl IntoResponse, ServerError> {
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
    catalog.delete_node(node.id).await.map_err(map_catalog_err)?;
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
