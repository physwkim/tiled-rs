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
            let search = if p.is_empty() {
                format!("{base}/api/v1/search/")
            } else {
                format!("{base}/api/v1/search/{p}")
            };
            let full = if p.is_empty() {
                format!("{base}/api/v1/container/full/")
            } else {
                format!("{base}/api/v1/container/full/{p}")
            };
            links.search = Some(search);
            links.full = Some(full);
        }
        StructureFamily::Array | StructureFamily::Sparse => {
            links.full = Some(format!("{base}/api/v1/array/full/{p}"));
            links
                .extra
                .insert("block".into(), format!("{base}/api/v1/array/block/{p}"));
        }
        StructureFamily::Ragged => {
            // Mirrors Python `links_for_ragged` (tiled/links.py:40-45): a ragged
            // node exposes `full` and `block`, like an array but on the
            // `/ragged/` route.
            links.full = Some(format!("{base}/api/v1/ragged/full/{p}"));
            links
                .extra
                .insert("block".into(), format!("{base}/api/v1/ragged/block/{p}"));
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

    // The router accepts `page[limit]=0` (no CatchPanicLayer), and the bare
    // `(count - 1) / limit` panics on divide-by-zero. Python `pagination_links`
    // never divides (tiled/server/core.py:122-147). Guard the division so the
    // illegal divisor cannot reach it: with `limit == 0` there is no meaningful
    // last-page offset, so `last` collapses to the first page (offset 0).
    let last_offset = if limit > 0 && count > 0 {
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
        assert_eq!(
            links.self_link.as_deref(),
            Some("http://localhost:8000/api/v1/metadata/")
        );
        assert_eq!(
            links.search.as_deref(),
            Some("http://localhost:8000/api/v1/search/")
        );
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

    #[test]
    fn test_pagination_links_zero_limit_does_not_panic() {
        // `page[limit]=0` is accepted by the router; the last-page division must
        // not divide by zero. `last` collapses to the first page (offset 0).
        let links = pagination_links("http://localhost:8000", "search", "", 0, 0, 100);
        assert_eq!(
            links.last.as_deref(),
            Some("http://localhost:8000/api/v1/search/?page[offset]=0&page[limit]=0")
        );
        assert_eq!(
            links.first.as_deref(),
            Some("http://localhost:8000/api/v1/search/?page[offset]=0&page[limit]=0")
        );
    }

    #[test]
    fn test_pagination_links_zero_limit_zero_count_does_not_panic() {
        // Both the `count == 0` and `limit == 0` guards exercised together.
        let links = pagination_links("http://localhost:8000", "search", "", 0, 0, 0);
        assert!(links.next.is_none());
        assert!(links.prev.is_none());
        assert_eq!(
            links.last.as_deref(),
            Some("http://localhost:8000/api/v1/search/?page[offset]=0&page[limit]=0")
        );
    }

    #[test]
    fn test_pagination_links_limit_exceeds_count() {
        // A single page larger than the result set: last == first, no next/prev.
        let links = pagination_links("http://localhost:8000", "search", "", 0, 50, 10);
        assert!(links.next.is_none());
        assert!(links.prev.is_none());
        assert_eq!(
            links.last.as_deref(),
            Some("http://localhost:8000/api/v1/search/?page[offset]=0&page[limit]=50")
        );
    }
}
