//! Link generation for node responses and pagination.
//!
//! Corresponds to `tiled/links.py` and `tiled/server/core.py:126-152`.

use crate::schemas::PaginationLinks;
use crate::structures::StructureFamily;

/// Default page size for paginated responses.
pub const DEFAULT_PAGE_SIZE: usize = 100;

/// Maximum page size allowed.
pub const MAX_PAGE_SIZE: usize = 300;

/// Maximum depth for recursive container browsing.
pub const DEPTH_LIMIT: usize = 5;

/// Generate links for a node based on its structure family.
///
/// Returns a `serde_json::Value` containing the appropriate link fields.
pub fn links_for_node(
    family: StructureFamily,
    base_url: &str,
    path: &str,
) -> serde_json::Value {
    let base = base_url.trim_end_matches('/');
    let path_trimmed = path.trim_start_matches('/');

    let self_link = if path_trimmed.is_empty() {
        format!("{base}/api/v1/metadata/")
    } else {
        format!("{base}/api/v1/metadata/{path_trimmed}")
    };

    match family {
        StructureFamily::Container => {
            let search = if path_trimmed.is_empty() {
                format!("{base}/api/v1/search/")
            } else {
                format!("{base}/api/v1/search/{path_trimmed}")
            };
            let full = if path_trimmed.is_empty() {
                format!("{base}/api/v1/entries/")
            } else {
                format!("{base}/api/v1/entries/{path_trimmed}")
            };
            serde_json::json!({
                "self": self_link,
                "search": search,
                "full": full,
            })
        }
        StructureFamily::Array => {
            let block = format!("{base}/api/v1/array/block/{path_trimmed}");
            let full = format!("{base}/api/v1/array/full/{path_trimmed}");
            serde_json::json!({
                "self": self_link,
                "full": full,
                "block": block,
            })
        }
        StructureFamily::Table => {
            let partition = format!("{base}/api/v1/table/partition/{path_trimmed}");
            let full = format!("{base}/api/v1/table/full/{path_trimmed}");
            serde_json::json!({
                "self": self_link,
                "full": full,
                "partition": partition,
            })
        }
        StructureFamily::Sparse => {
            let block = format!("{base}/api/v1/array/block/{path_trimmed}");
            let full = format!("{base}/api/v1/array/full/{path_trimmed}");
            serde_json::json!({
                "self": self_link,
                "full": full,
                "block": block,
            })
        }
        StructureFamily::Awkward => {
            let buffers = format!("{base}/api/v1/awkward/buffers/{path_trimmed}");
            let full = format!("{base}/api/v1/awkward/full/{path_trimmed}");
            serde_json::json!({
                "self": self_link,
                "full": full,
                "buffers": buffers,
            })
        }
    }
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
    let path_trimmed = path.trim_start_matches('/');

    let make_url = |o: usize, l: usize| -> String {
        if path_trimmed.is_empty() {
            format!("{base}/api/v1/{route}/?page[offset]={o}&page[limit]={l}")
        } else {
            format!("{base}/api/v1/{route}/{path_trimmed}?page[offset]={o}&page[limit]={l}")
        }
    };

    let self_link = make_url(offset, limit);

    let first = Some(make_url(0, limit));

    let last = if count > 0 {
        let last_offset = ((count.saturating_sub(1)) / limit) * limit;
        Some(make_url(last_offset, limit))
    } else {
        Some(make_url(0, limit))
    };

    let next = if offset + limit < count {
        Some(make_url(offset + limit, limit))
    } else {
        None
    };

    let prev = if offset > 0 {
        let prev_offset = offset.saturating_sub(limit);
        Some(make_url(prev_offset, limit))
    } else {
        None
    };

    PaginationLinks {
        self_link,
        first,
        last,
        next,
        prev,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_links_for_container_root() {
        let links = links_for_node(StructureFamily::Container, "http://localhost:8000", "");
        assert_eq!(links["self"], "http://localhost:8000/api/v1/metadata/");
        assert_eq!(links["search"], "http://localhost:8000/api/v1/search/");
    }

    #[test]
    fn test_links_for_array() {
        let links = links_for_node(StructureFamily::Array, "http://localhost:8000", "my_array");
        assert_eq!(
            links["self"],
            "http://localhost:8000/api/v1/metadata/my_array"
        );
        assert_eq!(
            links["block"],
            "http://localhost:8000/api/v1/array/block/my_array"
        );
    }

    #[test]
    fn test_links_for_table() {
        let links = links_for_node(StructureFamily::Table, "http://localhost:8000", "my_table");
        assert_eq!(
            links["partition"],
            "http://localhost:8000/api/v1/table/partition/my_table"
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
