//! Core response construction logic.
//!
//! Corresponds to `tiled/server/core.py` — `construct_resource`, `construct_entries_response`.

use tiled_core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_core::links;
use tiled_core::schemas::{
    ContainerMeta, NodeAttributes, NodeStructure, Resource, Response, SortingItem,
};

use crate::error::ServerError;

/// Walk the adapter tree to find a node at the given path.
///
/// Takes pre-split segments (already percent-decoded by the extractor) so
/// keys containing literal `/` (sent as `%2F`) reach `get()` intact.
///
/// Returns an **owned** [`AnyAdapter`] (a cheap `Arc` bump): the async
/// `ContainerAdapter::get` hands back owned children, so each hop resolves one
/// key lazily (one `fetch_child` for the SQL catalog) instead of materialising
/// a whole level just to borrow into it. A `get` that fails (DB error) is an
/// `Err`, never silently "not found".
#[tracing::instrument(skip(root))]
pub async fn walk_tree(
    root: &dyn ContainerAdapter,
    segments: &[String],
) -> Result<AnyAdapter, ServerError> {
    if segments.is_empty() {
        return Err(ServerError::NotFound("Use root directly".into()));
    }

    let last = segments.len() - 1;
    // First hop from the borrowed root; every later hop descends into the
    // owned container returned by the previous `get`.
    let mut current = root
        .get(&segments[0])
        .await?
        .ok_or_else(|| ServerError::NotFound(format!("Key not found: {}", segments[0])))?;

    for j in 1..=last {
        let parent = match current {
            AnyAdapter::Container(c) => c,
            _ => {
                return Err(ServerError::NotFound(format!(
                    "'{}' is not a container, cannot descend further",
                    segments[j - 1]
                )));
            }
        };
        current = parent
            .get(&segments[j])
            .await?
            .ok_or_else(|| ServerError::NotFound(format!("Key not found: {}", segments[j])))?;
    }

    Ok(current)
}

/// Compute ancestors list from a segment list.
///
/// Matches Python tiled's wire format: `["a", "b", "c"]` → `["a", "b"]`,
/// `["a"]` → `[]`, `[]` → `[]`.
pub fn ancestors_from_segments(segments: &[String]) -> Vec<String> {
    if segments.len() <= 1 {
        return vec![];
    }
    segments[..segments.len() - 1].to_vec()
}

/// Backwards-compat helper: split a slash-joined path and compute ancestors.
/// New code should prefer [`ancestors_from_segments`].
pub fn ancestors_from_path(path: &str) -> Vec<String> {
    let segments: Vec<String> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    ancestors_from_segments(&segments)
}

/// Default container sorting (ascending by insertion order).
#[inline]
fn default_sorting() -> Vec<SortingItem> {
    vec![SortingItem {
        key: "_".into(),
        direction: tiled_core::schemas::SortDirection::Ascending,
    }]
}

/// Construct a Resource for a given adapter.
///
/// Async because a container node's `structure_json` now awaits a child count
/// (a DB `count_children` for the SQL catalog).
pub async fn construct_resource(
    adapter: &AnyAdapter,
    id: &str,
    path: &str,
    base_url: &str,
) -> Result<Resource, ServerError> {
    let family = adapter.structure_family();
    let node_links = links::links_for_node(family, base_url, path);

    let sorting = match adapter {
        AnyAdapter::Container(_) => Some(default_sorting()),
        _ => None,
    };

    Ok(Resource {
        id: id.to_string(),
        attributes: NodeAttributes {
            ancestors: ancestors_from_path(path),
            structure_family: Some(family),
            specs: Some(adapter.specs().to_vec()),
            metadata: Some(adapter.metadata().clone()),
            structure: adapter.structure_json().await?,
            access_blob: None,
            sorting,
            data_sources: None,
        },
        links: node_links,
    })
}

/// Construct a Resource for the root container.
pub async fn construct_root_resource(
    root: &dyn ContainerAdapter,
    base_url: &str,
) -> Result<Resource, ServerError> {
    let node_links = links::links_for_node(root.structure_family(), base_url, "");
    let ns = NodeStructure {
        contents: None,
        count: root.len().await?,
    };

    Ok(Resource {
        id: String::new(),
        attributes: NodeAttributes {
            ancestors: vec![],
            structure_family: Some(root.structure_family()),
            specs: Some(root.specs().to_vec()),
            metadata: Some(root.metadata().clone()),
            structure: Some(
                serde_json::to_value(&ns).expect("NodeStructure is always serializable"),
            ),
            access_blob: None,
            sorting: Some(default_sorting()),
            data_sources: None,
        },
        links: node_links,
    })
}

/// Construct a paginated entries response for a container.
pub async fn construct_entries_response(
    container: &dyn ContainerAdapter,
    path: &str,
    base_url: &str,
    offset: usize,
    limit: usize,
    queries: &[tiled_core::queries::Query],
) -> Result<Response<Vec<Resource>>, ServerError> {
    // Apply search filters to get matching keys, then paginate. An unsupported
    // query variant surfaces as ServerError::UnsupportedQuery (HTTP 400),
    // matching Python tiled's UnsupportedQueryType handling.
    let matched_keys = container.search(queries).await?;
    let count = matched_keys.len();
    let path_trimmed = path.trim_matches('/');

    // Lazily resolve only the page's keys (one `get` each). A key that
    // vanished between `search` and `get` (concurrent delete) is skipped; a
    // `get` that errors propagates rather than silently dropping the entry.
    let mut entries: Vec<Resource> = Vec::new();
    for key in matched_keys.iter().skip(offset).take(limit) {
        let Some(adapter) = container.get(key).await? else {
            continue;
        };
        let child_path = if path_trimmed.is_empty() {
            key.clone()
        } else {
            format!("{path_trimmed}/{key}")
        };
        entries.push(construct_resource(&adapter, key, &child_path, base_url).await?);
    }

    let pagination = links::pagination_links(base_url, "search", path, offset, limit, count);

    Ok(Response {
        data: Some(entries),
        error: None,
        links: Some(
            serde_json::to_value(&pagination).expect("PaginationLinks is always serializable"),
        ),
        meta: Some(
            serde_json::to_value(&ContainerMeta { count })
                .expect("ContainerMeta is always serializable"),
        ),
    })
}
