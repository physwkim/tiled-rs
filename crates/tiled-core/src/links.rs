//! Link generation for node responses and pagination.
//!
//! Corresponds to `tiled/links.py` and `tiled/server/core.py:126-152`.

use crate::schemas::{NodeLinks, PaginationLinks};
use crate::structures::StructureFamily;

pub const DEFAULT_PAGE_SIZE: usize = 100;
pub const MAX_PAGE_SIZE: usize = 300;
pub const DEPTH_LIMIT: usize = 5;

/// Generate links for a node, returning a `NodeLinks` directly (no JSON round-trip).
pub fn links_for_node(family: StructureFamily, base_url: &str, path: &str) -> NodeLinks {
    let base = base_url.trim_end_matches('/');
    let p = path.trim_start_matches('/');

    let self_link = if p.is_empty() {
        format!("{base}/api/v1/metadata/")
    } else {
        format!("{base}/api/v1/metadata/{p}")
    };

    let mut links = NodeLinks {
        self_link: Some(self_link),
        ..Default::default()
    };

    match family {
        StructureFamily::Container => {
            links.search = Some(if p.is_empty() {
                format!("{base}/api/v1/search/")
            } else {
                format!("{base}/api/v1/search/{p}")
            });
            links.full = Some(if p.is_empty() {
                format!("{base}/api/v1/entries/")
            } else {
                format!("{base}/api/v1/entries/{p}")
            });
        }
        StructureFamily::Array | StructureFamily::Sparse => {
            links.full = Some(format!("{base}/api/v1/array/full/{p}"));
            links
                .extra
                .insert("block".into(), format!("{base}/api/v1/array/block/{p}"));
        }
        StructureFamily::Table => {
            links.full = Some(format!("{base}/api/v1/table/full/{p}"));
            links.extra.insert(
                "partition".into(),
                format!("{base}/api/v1/table/partition/{p}"),
            );
        }
        StructureFamily::Awkward => {
            links.full = Some(format!("{base}/api/v1/awkward/full/{p}"));
            links.extra.insert(
                "buffers".into(),
                format!("{base}/api/v1/awkward/buffers/{p}"),
            );
        }
    }

    links
}

/// Generate pagination links for a search/browse response.
pub fn pagination_links(
    base_url: &str,
    route: &str,
    path: &str,
    offset: usize,
    limit: usize,
    count: usize,
) -> PaginationLinks {
    let base = base_url.trim_end_matches('/');
    let p = path.trim_start_matches('/');

    let make_url = |o: usize, l: usize| -> String {
        if p.is_empty() {
            format!("{base}/api/v1/{route}/?page[offset]={o}&page[limit]={l}")
        } else {
            format!("{base}/api/v1/{route}/{p}?page[offset]={o}&page[limit]={l}")
        }
    };

    let last_offset = if count > 0 {
        ((count - 1) / limit) * limit
    } else {
        0
    };

    PaginationLinks {
        self_link: make_url(offset, limit),
        first: Some(make_url(0, limit)),
        last: Some(make_url(last_offset, limit)),
        next: (offset + limit < count).then(|| make_url(offset + limit, limit)),
        prev: (offset > 0).then(|| make_url(offset.saturating_sub(limit), limit)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_links_for_container_root() {
        let links = links_for_node(StructureFamily::Container, "http://localhost:8000", "");
        assert_eq!(links.self_link.as_deref(), Some("http://localhost:8000/api/v1/metadata/"));
        assert_eq!(links.search.as_deref(), Some("http://localhost:8000/api/v1/search/"));
    }

    #[test]
    fn test_links_for_array() {
        let links = links_for_node(StructureFamily::Array, "http://localhost:8000", "my_array");
        assert_eq!(
            links.self_link.as_deref(),
            Some("http://localhost:8000/api/v1/metadata/my_array")
        );
        assert_eq!(
            links.extra.get("block").map(|s| s.as_str()),
            Some("http://localhost:8000/api/v1/array/block/my_array")
        );
    }

    #[test]
    fn test_links_for_table() {
        let links = links_for_node(StructureFamily::Table, "http://localhost:8000", "my_table");
        assert_eq!(
            links.extra.get("partition").map(|s| s.as_str()),
            Some("http://localhost:8000/api/v1/table/partition/my_table")
        );
    }

    #[test]
    fn test_pagination_links_first_page() {
        let links = pagination_links("http://localhost:8000", "search", "", 0, 10, 100);
        assert_eq!(
            links.self_link,
            "http://localhost:8000/api/v1/search/?page[offset]=0&page[limit]=10"
        );
        assert!(links.next.is_some());
        assert!(links.prev.is_none());
        assert_eq!(
            links.last.as_deref(),
            Some("http://localhost:8000/api/v1/search/?page[offset]=90&page[limit]=10")
        );
    }

    #[test]
    fn test_pagination_links_middle_page() {
        let links = pagination_links("http://localhost:8000", "search", "", 50, 10, 100);
        assert!(links.next.is_some());
        assert!(links.prev.is_some());
    }

    #[test]
    fn test_pagination_links_last_page() {
        let links = pagination_links("http://localhost:8000", "search", "", 90, 10, 100);
        assert!(links.next.is_none());
        assert!(links.prev.is_some());
    }
}
