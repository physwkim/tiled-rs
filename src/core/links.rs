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
/// Emits EXACTLY the three keys upstream `pagination_links` builds —
/// `{self, first, next}` — and nothing else (`rg '"(last|prev)"' tiled/server`
/// → zero matches; upstream never emits `last`/`prev` on any page). Supports
/// both pagination modes:
/// - **Offset** (`cursor: None`): the in-memory/Mongo path and any non-default
///   sort. `self`/`first` are offset-relative; `next` advances the offset when
///   more rows remain.
/// - **Keyset cursor**: when the backend supplied a `next_cursor` (the SQL
///   catalog under a default sort, Python's `keys_page` cursor), `next` becomes
///   a `page[cursor]` link instead of `page[offset]`. A request that itself
///   arrived with a `cursor` echoes it in `self`. Mirrors Python
///   `pagination_links` (tiled/server/core.py:122-147).
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
        // Upstream omits the `page[offset]` segment entirely when the offset is
        // zero (core.py:126-129: `offset_or_cursor = ""` for offset 0/None), so
        // the first page's self/first/last read `?page[limit]=N`, not
        // `?page[offset]=0&page[limit]=N`. A non-zero offset keeps the segment.
        let offset_seg = if o > 0 {
            format!("page[offset]={o}&")
        } else {
            String::new()
        };
        if p.is_empty() {
            format!("{base}/api/v1/{route}/?{offset_seg}page[limit]={l}")
        } else {
            format!("{base}/api/v1/{route}/{p}?{offset_seg}page[limit]={l}")
        }
    };
    let cursor_url = |c: i64, l: usize| -> String {
        if p.is_empty() {
            format!("{base}/api/v1/{route}/?page[cursor]={c}&page[limit]={l}")
        } else {
            format!("{base}/api/v1/{route}/{p}?page[cursor]={c}&page[limit]={l}")
        }
    };

    // `next`: prefer the keyset cursor the backend supplied; otherwise fall
    // back to an offset link when more rows remain — but only for an offset
    // request, since a cursor request has no offset to advance. Upstream emits
    // no `last`/`prev`, so there is no last-page offset to compute (and hence
    // no `(count - 1) / limit` divide-by-zero to guard).
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
        next,
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
        // offset 0 → self omits the page[offset] segment (upstream parity).
        assert_eq!(
            links.self_link,
            "http://localhost:8000/api/v1/search/?page[limit]=10"
        );
        // More rows remain, so `next` advances the offset.
        assert_eq!(
            links.next.as_deref(),
            Some("http://localhost:8000/api/v1/search/?page[offset]=10&page[limit]=10")
        );
    }

    #[test]
    fn test_pagination_links_first_page_omits_zero_offset() {
        // Upstream builds `self`/`first` with `offset_or_cursor = ""` when the
        // offset is 0 (core.py:126-129), so the first page's self/first read
        // `?page[limit]=N`, NOT `?page[offset]=0&page[limit]=N`. `first`
        // (always offset 0) never carries an offset segment. A non-zero offset
        // still carries it.
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
            links.self_link, "http://localhost:8000/api/v1/search/?page[limit]=10",
            "offset-0 self omits page[offset]"
        );
        assert_eq!(
            links.first.as_deref(),
            Some("http://localhost:8000/api/v1/search/?page[limit]=10"),
            "first is always offset 0, so never carries page[offset]"
        );
        // A non-zero offset (the `next` link here, offset 10) keeps the segment.
        assert_eq!(
            links.next.as_deref(),
            Some("http://localhost:8000/api/v1/search/?page[offset]=10&page[limit]=10"),
            "non-zero offset still carries page[offset]"
        );
    }

    #[test]
    fn test_pagination_links_first_page_nested_path_omits_zero_offset() {
        // Same zero-offset rule with a non-empty path segment.
        let links = pagination_links(
            "http://localhost:8000",
            "search",
            "expt",
            None,
            0,
            10,
            None,
            2,
        );
        assert_eq!(
            links.self_link,
            "http://localhost:8000/api/v1/search/expt?page[limit]=10"
        );
        assert_eq!(
            links.first.as_deref(),
            Some("http://localhost:8000/api/v1/search/expt?page[limit]=10")
        );
    }

    #[test]
    fn test_pagination_links_never_emits_last_or_prev() {
        // Upstream `pagination_links` (core.py:122-147) builds EXACTLY
        // {self, first, next}; `rg '"(last|prev)"' tiled/server` → zero
        // matches. The port must never emit `last`/`prev` on ANY page. Check
        // the two pages that historically populated them: a first/offset page
        // (`last` was Some) and a middle page (`prev` was Some).
        for (offset, count) in [(0usize, 100usize), (50, 100)] {
            let links = pagination_links(
                "http://localhost:8000",
                "search",
                "",
                None,
                offset,
                10,
                None,
                count,
            );
            let obj = serde_json::to_value(&links).unwrap();
            let obj = obj.as_object().unwrap();
            let keys: Vec<&str> = obj.keys().map(String::as_str).collect();
            assert!(obj.contains_key("self"), "self present (offset {offset})");
            assert!(obj.contains_key("first"), "first present (offset {offset})");
            assert!(obj.contains_key("next"), "next present (offset {offset})");
            assert!(
                !obj.contains_key("last"),
                "last must NOT be emitted (offset {offset}): keys={keys:?}"
            );
            assert!(
                !obj.contains_key("prev"),
                "prev must NOT be emitted (offset {offset}): keys={keys:?}"
            );
        }
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
        // Middle page: more rows remain, so `next` advances the offset.
        assert_eq!(
            links.next.as_deref(),
            Some("http://localhost:8000/api/v1/search/?page[offset]=60&page[limit]=10")
        );
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
        // Terminal page: no rows remain, so `next` is None (serialized null).
        assert!(links.next.is_none());
    }

    #[test]
    fn test_pagination_links_zero_limit_does_not_panic() {
        // `page[limit]=0` is accepted by the router. There is no longer any
        // `(count - 1) / limit` division (the `last` link that needed it was
        // removed), so limit=0 simply yields well-formed self/first links.
        let links = pagination_links("http://localhost:8000", "search", "", None, 0, 0, None, 100);
        // offset 0 → no page[offset] segment (upstream parity).
        assert_eq!(
            links.self_link,
            "http://localhost:8000/api/v1/search/?page[limit]=0"
        );
        assert_eq!(
            links.first.as_deref(),
            Some("http://localhost:8000/api/v1/search/?page[limit]=0")
        );
    }

    #[test]
    fn test_pagination_links_zero_limit_zero_count_does_not_panic() {
        // Both edge inputs together: limit=0 and count=0. No division, no
        // panic; `next` is None because no rows remain.
        let links = pagination_links("http://localhost:8000", "search", "", None, 0, 0, None, 0);
        assert!(links.next.is_none());
        assert_eq!(
            links.first.as_deref(),
            Some("http://localhost:8000/api/v1/search/?page[limit]=0")
        );
    }

    #[test]
    fn test_pagination_links_limit_exceeds_count() {
        // A single page larger than the result set: self == first, no next.
        let links = pagination_links("http://localhost:8000", "search", "", None, 0, 50, None, 10);
        assert!(links.next.is_none());
        // offset 0 → no page[offset] segment.
        assert_eq!(
            links.first.as_deref(),
            Some("http://localhost:8000/api/v1/search/?page[limit]=50")
        );
    }

    #[test]
    fn test_pagination_next_uses_cursor_when_supplied() {
        // Default-sort catalog search: the backend hands back a keyset cursor,
        // so `next` is a page[cursor] link (N3 parity with Python).
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
        // This is an offset request (cursor arg None) at offset 0 → self omits
        // page[offset]; only `next` carries the keyset cursor.
        assert_eq!(
            links.self_link,
            "http://localhost:8000/api/v1/search/?page[limit]=2"
        );
    }

    #[test]
    fn test_pagination_cursor_request_echoes_cursor_in_self() {
        // A request that arrived with page[cursor]: self echoes the cursor and
        // next carries the following cursor (a keyset page is forward-only;
        // upstream emits no last/prev on any page).
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
