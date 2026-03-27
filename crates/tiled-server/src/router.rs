//! Route handlers for the Tiled API.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::Json;

use tiled_core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_core::links;
use tiled_core::schemas::{About, AboutAuthentication, Response};

use crate::core;
use crate::error::ServerError;
use crate::extractors::BaseUrl;
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Operational endpoints
// ---------------------------------------------------------------------------

pub async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

pub async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    let count = state.root_tree.len();
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
        meta: HashMap::from([(
            "root_path".into(),
            serde_json::Value::String(String::new()),
        )]),
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
    metadata(state, Path(String::new()), base_url).await
}

pub async fn metadata(
    State(state): State<AppState>,
    Path(path): Path<String>,
    BaseUrl(base_url): BaseUrl,
) -> Result<impl IntoResponse, ServerError> {
    let path = path.trim_matches('/');

    let resource = if path.is_empty() {
        core::construct_root_resource(state.root_tree.as_ref(), &base_url)
    } else {
        let adapter = core::walk_tree(state.root_tree.as_ref(), path)?;
        let id = path.rsplit('/').next().unwrap_or(path);
        core::construct_resource(adapter, id, path, &base_url)
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
    search(state, Path(String::new()), params, base_url).await
}

pub async fn search(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    BaseUrl(base_url): BaseUrl,
) -> Result<impl IntoResponse, ServerError> {
    let path = path.trim_matches('/');

    let offset: usize = params
        .get("page[offset]")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let limit: usize = params
        .get("page[limit]")
        .and_then(|v| v.parse().ok())
        .unwrap_or(links::DEFAULT_PAGE_SIZE)
        .min(links::MAX_PAGE_SIZE);

    // Parse query filters from URL params
    let filter_params: Vec<(String, String)> = params
        .iter()
        .filter(|(k, _)| k.starts_with("filter["))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let queries = tiled_core::queries::decode_query_filters(&filter_params);

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

    let resp = core::construct_entries_response(
        container, path, &base_url, offset, limit, &queries,
    );
    Ok(Json(resp))
}

// ---------------------------------------------------------------------------
// GET /api/v1/array/block/{*path}
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
// GET /api/v1/table/partition/{*path}
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

    let fields: Option<Vec<String>> = params.get("field").map(|f| {
        f.split(',').map(|s| s.trim().to_string()).collect()
    });

    let table = table_adapter
        .read_partition(partition, fields.as_deref())
        .await
        .map_err(ServerError::from)?;

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
