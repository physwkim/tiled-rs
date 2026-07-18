//! Link generation for node responses and pagination.
//!
//! Corresponds to `tiled/links.py` and `tiled/server/core.py:126-152`.

use crate::core::schemas::{NodeLinks, PaginationLinks};
use crate::core::structures::StructureFamily;

pub const DEFAULT_PAGE_SIZE: usize = 100;
pub const MAX_PAGE_SIZE: usize = 300;
/// Maximum walk depth shared by the `?max_depth=` inlining gate (metadata /
/// search) and the container/full zip export — mirrors Python `DEPTH_LIMIT = 5`
/// (`tiled/server/core.py:62`). It bounds both the accepted `max_depth` query
/// value (`Query(None, ge=0, le=DEPTH_LIMIT)`, router.py:322/460) and the
/// `depth <= DEPTH_LIMIT` clause of the inline gate (core.py:516).
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
        StructureFamily::Table => {
            links.full = Some(format!("{base}/api/v1/table/full/{p}"));
            links.extra.insert(
                "partition".into(),
                format!("{base}/api/v1/table/partition/{p}"),
            );
        }
        StructureFamily::Ragged => {
            // Ragged IS servable end-to-end: `AnyAdapter::Ragged` wraps the
            // concrete `RaggedAdapter`, the `/ragged/full` route reads it, and
            // the registry serializes it (JSON/zip/Arrow/Parquet). Mirrors
            // Python `links_for_ragged` (tiled/links.py:40-45): `full` + `block`.
            links.full = Some(format!("{base}/api/v1/ragged/full/{p}"));
            links
                .extra
                .insert("block".into(), format!("{base}/api/v1/ragged/block/{p}"));
        }
        // Mirrors Python `links_for_awkward` (tiled/links.py:26-30): advertise
        // `full` (GET /awkward/full) and `buffers` (POST /awkward/buffers).
        // Both routes are wired in app.rs and handled in router.rs.
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
///
/// Supports both pagination modes:
/// - **Offset** (`cursor: None`): the in-memory/Mongo path and any non-default
///   sort. `self`/`first`/`last`/`prev`/`next` are offset-relative, exactly as
///   before.
/// - **Keyset cursor**: when the backend supplied a `next_cursor` (the SQL
///   catalog under a default sort, Python's `keys_page` cursor), `next` becomes
///   a `page[cursor]` link instead of `page[offset]`. A request that itself
///   arrived with a `cursor` echoes it in `self`; `last`/`prev` are omitted
///   because a keyset page is forward-only. Mirrors Python `pagination_links`
///   (tiled/server/core.py:122-147).
#[allow(clippy::too_many_arguments)]
pub fn pagination_links(
    base_url: &str,
    route: &str,
    path: &str,
    cursor: Option<i64>,
    offset: usize,
    limit: usize,
    next_cursor: Option<i64>,
    count: usize,
) -> PaginationLinks {
    let base = base_url.trim_end_matches('/');
    let p = path.trim_start_matches('/');

    let offset_url = |o: usize, l: usize| -> String {
        if p.is_empty() {
            format!("{base}/api/v1/{route}/?page[offset]={o}&page[limit]={l}")
        } else {
            format!("{base}/api/v1/{route}/{p}?page[offset]={o}&page[limit]={l}")
        }
    };
    let cursor_url = |c: i64, l: usize| -> String {
        if p.is_empty() {
            format!("{base}/api/v1/{route}/?page[cursor]={c}&page[limit]={l}")
        } else {
            format!("{base}/api/v1/{route}/{p}?page[cursor]={c}&page[limit]={l}")
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

    // `next`: prefer the keyset cursor the backend supplied; otherwise fall
    // back to an offset link when more rows remain — but only for an offset
    // request, since a cursor request has no offset to advance.
    let next = if let Some(nc) = next_cursor {
        Some(cursor_url(nc, limit))
    } else if cursor.is_none() && offset + limit < count {
        Some(offset_url(offset + limit, limit))
    } else {
        None
    };

    PaginationLinks {
        self_link: match cursor {
            Some(c) => cursor_url(c, limit),
            None => offset_url(offset, limit),
        },
        first: Some(offset_url(0, limit)),
        last: cursor.is_none().then(|| offset_url(last_offset, limit)),
        next,
        prev: (cursor.is_none() && offset > 0)
            .then(|| offset_url(offset.saturating_sub(limit), limit)),
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
    fn test_links_for_ragged_advertises_full_and_block() {
        // Ragged is servable end-to-end (AnyAdapter::Ragged + /ragged/full +
        // serializers), so it advertises full + block like an array, on the
        // /ragged/ route (parity with Python links_for_ragged).
        let links = links_for_node(
            StructureFamily::Ragged,
            "http://localhost:8000",
            "my_ragged",
        );
        assert_eq!(
            links.self_link.as_deref(),
            Some("http://localhost:8000/api/v1/metadata/my_ragged")
        );
        assert_eq!(
            links.full.as_deref(),
            Some("http://localhost:8000/api/v1/ragged/full/my_ragged")
        );
        assert_eq!(
            links.extra.get("block").map(|s| s.as_str()),
            Some("http://localhost:8000/api/v1/ragged/block/my_ragged")
        );
    }

    #[test]
    fn test_links_for_awkward_advertises_full_and_buffers() {
        // Awkward is servable end-to-end (AwkwardAdapter + /awkward/full +
        // /awkward/buffers routes + application/zip serializer).  Mirrors Python
        // `links_for_awkward` (tiled/links.py:26-30): `full` + `buffers`.
        let links = links_for_node(
            StructureFamily::Awkward,
            "http://localhost:8000",
            "my_awkward",
        );
        assert_eq!(
            links.self_link.as_deref(),
            Some("http://localhost:8000/api/v1/metadata/my_awkward")
        );
        assert_eq!(
            links.full.as_deref(),
            Some("http://localhost:8000/api/v1/awkward/full/my_awkward")
        );
        assert_eq!(
            links.extra.get("buffers").map(|s| s.as_str()),
            Some("http://localhost:8000/api/v1/awkward/buffers/my_awkward")
        );
    }

    #[test]
    fn test_pagination_links_first_page() {
        let links = pagination_links(
            "http://localhost:8000",
            "search",
            "",
            None,
            0,
            10,
            None,
            100,
        );
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
        let links = pagination_links(
            "http://localhost:8000",
            "search",
            "",
            None,
            50,
            10,
            None,
            100,
        );
        assert!(links.next.is_some());
        assert!(links.prev.is_some());
    }

    #[test]
    fn test_pagination_links_last_page() {
        let links = pagination_links(
            "http://localhost:8000",
            "search",
            "",
            None,
            90,
            10,
            None,
            100,
        );
        assert!(links.next.is_none());
        assert!(links.prev.is_some());
    }

    #[test]
    fn test_pagination_links_zero_limit_does_not_panic() {
        // `page[limit]=0` is accepted by the router; the last-page division must
        // not divide by zero. `last` collapses to the first page (offset 0).
        let links = pagination_links("http://localhost:8000", "search", "", None, 0, 0, None, 100);
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
        let links = pagination_links("http://localhost:8000", "search", "", None, 0, 0, None, 0);
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
        let links = pagination_links("http://localhost:8000", "search", "", None, 0, 50, None, 10);
        assert!(links.next.is_none());
        assert!(links.prev.is_none());
        assert_eq!(
            links.last.as_deref(),
            Some("http://localhost:8000/api/v1/search/?page[offset]=0&page[limit]=50")
        );
    }

    #[test]
    fn test_pagination_next_uses_cursor_when_supplied() {
        // Default-sort catalog search: the backend hands back a keyset cursor,
        // so `next` is a page[cursor] link (N3 parity with Python). last/prev
        // stay offset-relative for this offset request; first is unchanged.
        let links = pagination_links(
            "http://localhost:8000",
            "search",
            "",
            None,
            0,
            2,
            Some(42),
            10,
        );
        assert_eq!(
            links.next.as_deref(),
            Some("http://localhost:8000/api/v1/search/?page[cursor]=42&page[limit]=2")
        );
        assert_eq!(
            links.self_link,
            "http://localhost:8000/api/v1/search/?page[offset]=0&page[limit]=2"
        );
        assert!(links.last.is_some());
    }

    #[test]
    fn test_pagination_cursor_request_is_forward_only() {
        // A request that arrived with page[cursor]: self echoes the cursor,
        // next carries the following cursor, and last/prev are omitted (a
        // keyset page cannot cheaply seek backwards).
        let links = pagination_links(
            "http://localhost:8000",
            "search",
            "",
            Some(7),
            0,
            2,
            Some(42),
            10,
        );
        assert_eq!(
            links.self_link,
            "http://localhost:8000/api/v1/search/?page[cursor]=7&page[limit]=2"
        );
        assert_eq!(
            links.next.as_deref(),
            Some("http://localhost:8000/api/v1/search/?page[cursor]=42&page[limit]=2")
        );
        assert!(links.last.is_none());
        assert!(links.prev.is_none());
    }

    #[test]
    fn test_pagination_cursor_request_last_page_has_no_next() {
        // No next_cursor on a cursor request → no next link (forward-only end).
        let links = pagination_links(
            "http://localhost:8000",
            "search",
            "",
            Some(7),
            0,
            2,
            None,
            10,
        );
        assert!(links.next.is_none());
    }
}
