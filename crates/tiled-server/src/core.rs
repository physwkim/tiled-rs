//! Core response construction logic.
//!
//! Corresponds to `tiled/server/core.py` — `construct_resource`, `construct_entries_response`.

use tiled_core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_core::links;
use tiled_core::schemas::{
    ContainerMeta, NodeAttributes, NodeLinks, NodeStructure, Resource, Response, SortingItem,
};

use crate::error::ServerError;

/// Walk the adapter tree to find a node at the given path.
pub fn walk_tree<'a>(
    root: &'a dyn ContainerAdapter,
    path: &str,
) -> Result<&'a AnyAdapter, ServerError> {
    let path = path.trim_matches('/');
    if path.is_empty() {
        // Return root as AnyAdapter isn't possible directly, caller handles root specially
        return Err(ServerError::NotFound("Use root directly".into()));
    }

    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let mut current_container: &dyn ContainerAdapter = root;

    for (i, segment) in segments.iter().enumerate() {
        let adapter = current_container
            .get(segment)
            .ok_or_else(|| ServerError::NotFound(format!("Key not found: {segment}")))?;

        if i == segments.len() - 1 {
            // This is the last segment — return whatever we found
            return Ok(adapter);
        }

        // Need to descend further — must be a container
        match adapter {
            AnyAdapter::Container(c) => {
                current_container = c.as_ref();
            }
            _ => {
                return Err(ServerError::NotFound(format!(
                    "'{segment}' is not a container, cannot descend further"
                )));
            }
        }
    }

    Err(ServerError::NotFound("Path not found".into()))
}

/// Compute ancestors list from a path.
///
/// For a node at path "a/b/c", returns `["", "a", "a/b"]` — the paths of
/// each ancestor container from root downward, matching the Python wire format.
/// Root node itself has no ancestors (returns `[]`).
pub fn ancestors_from_path(path: &str) -> Vec<String> {
    let path = path.trim_matches('/');
    if path.is_empty() {
        return vec![];
    }
    let parts: Vec<&str> = path.split('/').collect();
    // For N path segments there are N ancestors (root + each intermediate).
    // "a"       → [""]
    // "a/b"     → ["", "a"]
    // "a/b/c"   → ["", "a", "a/b"]
    let mut ancestors = Vec::with_capacity(parts.len());
    for i in 0..parts.len() {
        if i == 0 {
            ancestors.push(String::new());
        } else {
            ancestors.push(parts[..i].join("/"));
        }
    }
    ancestors
}

/// Construct a Resource for a given adapter.
pub fn construct_resource(
    adapter: &AnyAdapter,
    id: &str,
    path: &str,
    base_url: &str,
) -> Resource {
    let family = adapter.structure_family();
    let metadata = adapter.metadata().clone();
    let ancestors = ancestors_from_path(path);

    let structure = adapter.structure_json();

    let sorting = match adapter {
        AnyAdapter::Container(_) => Some(vec![SortingItem {
            key: "_".into(),
            direction: tiled_core::schemas::SortDirection::Ascending,
        }]),
        _ => None,
    };

    let link_value = links::links_for_node(family, base_url, path);
    let links_map: std::collections::HashMap<String, String> =
        serde_json::from_value(link_value.clone()).unwrap_or_default();

    let mut node_links = NodeLinks::default();
    if let Some(s) = links_map.get("self") {
        node_links.self_link = Some(s.clone());
    }
    if let Some(s) = links_map.get("search") {
        node_links.search = Some(s.clone());
    }
    if let Some(s) = links_map.get("full") {
        node_links.full = Some(s.clone());
    }
    for (k, v) in &links_map {
        if k != "self" && k != "search" && k != "full" {
            node_links.extra.insert(k.clone(), v.clone());
        }
    }

    Resource {
        id: id.to_string(),
        attributes: NodeAttributes {
            ancestors,
            structure_family: Some(family),
            specs: Some(adapter.specs().to_vec()),
            metadata: Some(metadata),
            structure,
            access_blob: None,
            sorting,
            data_sources: None,
        },
        links: node_links,
    }
}

/// Construct a Resource for the root container.
pub fn construct_root_resource(
    root: &dyn ContainerAdapter,
    base_url: &str,
) -> Resource {
    let family = root.structure_family();
    let metadata = root.metadata().clone();

    let ns = NodeStructure {
        contents: None,
        count: root.len(),
    };

    let link_value = links::links_for_node(family, base_url, "");
    let links_map: std::collections::HashMap<String, String> =
        serde_json::from_value(link_value).unwrap_or_default();

    let mut node_links = NodeLinks::default();
    if let Some(s) = links_map.get("self") {
        node_links.self_link = Some(s.clone());
    }
    if let Some(s) = links_map.get("search") {
        node_links.search = Some(s.clone());
    }
    if let Some(s) = links_map.get("full") {
        node_links.full = Some(s.clone());
    }

    Resource {
        id: String::new(),
        attributes: NodeAttributes {
            ancestors: vec![],
            structure_family: Some(family),
            specs: Some(root.specs().to_vec()),
            metadata: Some(metadata),
            structure: Some(serde_json::to_value(ns).unwrap_or_default()),
            access_blob: None,
            sorting: Some(vec![SortingItem {
                key: "_".into(),
                direction: tiled_core::schemas::SortDirection::Ascending,
            }]),
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
) -> Response<Vec<Resource>> {
    let count = container.len();
    let keys = container.keys();
    let page_keys: Vec<&String> = keys.iter().skip(offset).take(limit).collect();

    let mut entries = Vec::new();
    for key in &page_keys {
        if let Some(adapter) = container.get(key) {
            let child_path = if path.is_empty() || path == "/" {
                key.to_string()
            } else {
                format!("{}/{}", path.trim_matches('/'), key)
            };
            let resource = construct_resource(adapter, key, &child_path, base_url);
            entries.push(resource);
        }
    }

    let pagination = links::pagination_links(base_url, "search", path, offset, limit, count);

    Response {
        data: Some(entries),
        error: None,
        links: Some(serde_json::to_value(pagination).unwrap_or_default()),
        meta: Some(serde_json::to_value(ContainerMeta { count }).unwrap_or_default()),
    }
}
