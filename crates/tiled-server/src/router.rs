//! Route handlers for the Tiled API.
//!
//! Corresponds to `tiled/server/router.py`.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::Json;

use tiled_core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_core::links;
use tiled_core::schemas::{About, AboutAuthentication, Resource, Response};

use crate::core;
use crate::error::ServerError;
use crate::state::AppState;

// ---------------------------------------------------------------------------
// GET /api/v1/ — About endpoint
// ---------------------------------------------------------------------------

pub async fn about(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let base_url = state.resolve_base_url(&headers);
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
        meta: HashMap::from([(
            "root_path".into(),
            serde_json::Value::String(String::new()),
        )]),
    };

    Json(about)
}

// ---------------------------------------------------------------------------
// GET /api/v1/metadata/{*path} — Single node metadata
// ---------------------------------------------------------------------------

pub async fn metadata_root(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    metadata_inner(state, String::new(), headers).await
}

pub async fn metadata(
    State(state): State<AppState>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    metadata_inner(state, path, headers).await
}

async fn metadata_inner(
    state: AppState,
    path: String,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let base_url = state.resolve_base_url(&headers);
    let path = path.trim_matches('/');

    let resource = if path.is_empty() {
        core::construct_root_resource(state.root_tree.as_ref(), &base_url)
    } else {
        let adapter = core::walk_tree(state.root_tree.as_ref(), path)?;
        let id = path.rsplit('/').next().unwrap_or(path);
        core::construct_resource(adapter, id, path, &base_url)
    };

    let resp: Response<Resource> = Response {
        data: Some(resource),
        error: None,
        links: None,
        meta: None,
    };

    Ok(Json(resp))
}

// ---------------------------------------------------------------------------
// GET /api/v1/search/{*path} — Browse/search container
// ---------------------------------------------------------------------------

pub async fn search_root(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    search_inner(state, String::new(), params, headers).await
}

pub async fn search(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    search_inner(state, path, params, headers).await
}

async fn search_inner(
    state: AppState,
    path: String,
    params: HashMap<String, String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let base_url = state.resolve_base_url(&headers);
    let path = path.trim_matches('/');

    // Parse pagination params
    let offset: usize = params
        .get("page[offset]")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let limit: usize = params
        .get("page[limit]")
        .and_then(|v| v.parse().ok())
        .unwrap_or(links::DEFAULT_PAGE_SIZE)
        .min(links::MAX_PAGE_SIZE);

    // Find the container
    let container: &dyn ContainerAdapter = if path.is_empty() {
        state.root_tree.as_ref()
    } else {
        let adapter = core::walk_tree(state.root_tree.as_ref(), path)?;
        match adapter {
            AnyAdapter::Container(c) => c.as_ref(),
            _ => {
                return Err(ServerError::Validation(format!(
                    "'{path}' is not a container"
                )));
            }
        }
    };

    let resp = core::construct_entries_response(container, path, &base_url, offset, limit);

    Ok(Json(resp))
}

// ---------------------------------------------------------------------------
// GET /api/v1/array/block/{*path} — Array block data
// ---------------------------------------------------------------------------

pub async fn array_block(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let path = path.trim_matches('/');

    let adapter = core::walk_tree(state.root_tree.as_ref(), path)?;
    let array_adapter = match adapter {
        AnyAdapter::Array(a) => a.as_ref(),
        _ => {
            return Err(ServerError::Validation(format!(
                "'{path}' is not an array"
            )));
        }
    };

    // Parse block parameter (comma-separated indices)
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

    let slice = tiled_core::ndslice::NDSlice::empty();
    let data = array_adapter
        .read_block(&block, &slice)
        .await
        .map_err(ServerError::from)?;

    // Content negotiation
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

    let body = if let Some(serializer) =
        state
            .serialization_registry
            .dispatch(tiled_core::structures::StructureFamily::Array, &media_type)
    {
        let ser_meta = serde_json::json!({
            "itemsize": data.dtype.element_size(),
            "kind": String::from(data.dtype.kind.to_numpy_char()),
        });
        serializer(&data.data, &ser_meta)
            .map_err(|e| ServerError::Internal(e.to_string()))?
    } else {
        data.data
    };

    Ok((
        [(axum::http::header::CONTENT_TYPE, media_type)],
        body,
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// GET /api/v1/table/partition/{*path} — Table partition data
// ---------------------------------------------------------------------------

pub async fn table_partition(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, ServerError> {
    let path = path.trim_matches('/');

    let adapter = core::walk_tree(state.root_tree.as_ref(), path)?;
    let table_adapter = match adapter {
        AnyAdapter::Table(t) => t.as_ref(),
        _ => {
            return Err(ServerError::Validation(format!(
                "'{path}' is not a table"
            )));
        }
    };

    let partition: usize = params
        .get("partition")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    // Parse optional field selection
    let fields: Option<Vec<String>> = params.get("field").map(|f| {
        f.split(',').map(|s| s.trim().to_string()).collect()
    });

    let table = table_adapter
        .read_partition(partition, fields.as_deref())
        .await
        .map_err(ServerError::from)?;

    // Serialize as Arrow IPC
    let mut buf = Vec::new();
    {
        let mut writer =
            arrow::ipc::writer::FileWriter::try_new(&mut buf, &table.schema)
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
