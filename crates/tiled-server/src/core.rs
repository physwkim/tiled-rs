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
#[tracing::instrument(skip(root))]
pub fn walk_tree<'a>(
    root: &'a dyn ContainerAdapter,
    segments: &[String],
) -> Result<&'a AnyAdapter, ServerError> {
    if segments.is_empty() {
        return Err(ServerError::NotFound("Use root directly".into()));
    }

    let mut current_container: &dyn ContainerAdapter = root;
    let last = segments.len() - 1;
    for (i, segment) in segments.iter().enumerate() {
        let adapter = current_container
            .get(segment)
            .ok_or_else(|| ServerError::NotFound(format!("Key not found: {segment}")))?;

        if i == last {
            return Ok(adapter);
        }

        match adapter {
            AnyAdapter::Container(c) => current_container = c.as_ref(),
            _ => {
                return Err(ServerError::NotFound(format!(
                    "'{segment}' is not a container, cannot descend further"
                )));
            }
        }
    }

    Err(ServerError::NotFound("Path not found".into()))
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
pub fn construct_resource(adapter: &AnyAdapter, id: &str, path: &str, base_url: &str) -> Resource {
    let family = adapter.structure_family();
    let node_links = links::links_for_node(family, base_url, path);

    let sorting = match adapter {
        AnyAdapter::Container(_) => Some(default_sorting()),
        _ => None,
    };

    Resource {
        id: id.to_string(),
        attributes: NodeAttributes {
            ancestors: ancestors_from_path(path),
            structure_family: Some(family),
            specs: Some(adapter.specs().to_vec()),
            metadata: Some(adapter.metadata().clone()),
            structure: adapter.structure_json(),
            access_blob: None,
            sorting,
            data_sources: None,
        },
        links: node_links,
    }
}

/// Construct a Resource for the root container.
pub fn construct_root_resource(root: &dyn ContainerAdapter, base_url: &str) -> Resource {
    let node_links = links::links_for_node(root.structure_family(), base_url, "");
    let ns = NodeStructure {
        contents: None,
        count: root.len(),
    };

    Resource {
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
    }
}

/// Construct a paginated entries response for a container.
pub fn construct_entries_response(
    container: &dyn ContainerAdapter,
    path: &str,
    base_url: &str,
    offset: usize,
    limit: usize,
    queries: &[tiled_core::queries::Query],
) -> Response<Vec<Resource>> {
    // Apply search filters to get matching keys, then paginate.
    let matched_keys = container.search(queries);
    let count = matched_keys.len();
    let path_trimmed = path.trim_matches('/');

    let entries: Vec<Resource> = matched_keys
        .iter()
        .skip(offset)
        .take(limit)
        .filter_map(|key| {
            let adapter = container.get(key)?;
            let child_path = if path_trimmed.is_empty() {
                key.clone()
            } else {
                format!("{path_trimmed}/{key}")
            };
            Some(construct_resource(adapter, key, &child_path, base_url))
        })
        .collect();

    let pagination = links::pagination_links(base_url, "search", path, offset, limit, count);

    Response {
        data: Some(entries),
        error: None,
        links: Some(
            serde_json::to_value(&pagination).expect("PaginationLinks is always serializable"),
        ),
        meta: Some(
            serde_json::to_value(&ContainerMeta { count })
                .expect("ContainerMeta is always serializable"),
        ),
    }
}
