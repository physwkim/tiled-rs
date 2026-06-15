//! In-memory container adapter backed by an IndexMap.
//!
//! Corresponds to `tiled/adapters/mapping.py:MapAdapter`.

use std::sync::Arc;

use indexmap::IndexMap;

use tiled_core::adapters::{AnyAdapter, BaseAdapter, BoxFuture, ContainerAdapter};
use tiled_core::queries::{Query, UnsupportedQuery};
use tiled_core::schemas::{SortDirection, SortingItem};
use tiled_core::structures::{ContainerStructure, Spec, StructureFamily};

/// An in-memory container adapter using an ordered map.
pub struct MapAdapter {
    mapping: Arc<IndexMap<String, AnyAdapter>>,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    sorting: Vec<SortingItem>,
    must_revalidate: bool,
}

impl MapAdapter {
    pub fn new(
        mapping: IndexMap<String, AnyAdapter>,
        metadata: serde_json::Value,
        specs: Vec<Spec>,
    ) -> Self {
        Self {
            mapping: Arc::new(mapping),
            metadata,
            specs,
            sorting: vec![SortingItem {
                key: "_".into(),
                direction: SortDirection::Ascending,
            }],
            must_revalidate: true,
        }
    }

    pub fn with_sorting(mut self, sorting: Vec<SortingItem>) -> Self {
        self.sorting = sorting;
        self
    }

    pub fn with_must_revalidate(mut self, must_revalidate: bool) -> Self {
        self.must_revalidate = must_revalidate;
        self
    }

    #[inline]
    pub fn must_revalidate(&self) -> bool {
        self.must_revalidate
    }

    #[inline]
    pub fn sorting(&self) -> &[SortingItem] {
        &self.sorting
    }

    /// Iterate over a paginated range of (key, adapter) pairs.
    pub fn items_range(
        &self,
        offset: usize,
        limit: usize,
    ) -> impl Iterator<Item = (&str, &AnyAdapter)> {
        self.mapping
            .iter()
            .skip(offset)
            .take(limit)
            .map(|(k, v)| (k.as_str(), v))
    }
}

impl BaseAdapter for MapAdapter {
    #[inline]
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Container
    }

    #[inline]
    fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }

    #[inline]
    fn specs(&self) -> &[Spec] {
        &self.specs
    }
}

impl ContainerAdapter for MapAdapter {
    fn structure(&self) -> BoxFuture<'_, tiled_core::error::Result<ContainerStructure>> {
        Box::pin(async move {
            Ok(ContainerStructure {
                keys: self.mapping.keys().cloned().collect(),
            })
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, tiled_core::error::Result<Option<AnyAdapter>>> {
        // In-memory: an owned clone is a cheap `Arc` refcount bump.
        Box::pin(async move { Ok(self.mapping.get(key).cloned()) })
    }

    fn keys(&self) -> BoxFuture<'_, tiled_core::error::Result<Vec<String>>> {
        Box::pin(async move { Ok(self.mapping.keys().cloned().collect()) })
    }

    fn len(&self) -> BoxFuture<'_, tiled_core::error::Result<usize>> {
        Box::pin(async move { Ok(self.mapping.len()) })
    }

    fn search<'a>(
        &'a self,
        queries: &'a [Query],
    ) -> BoxFuture<'a, tiled_core::error::Result<Vec<String>>> {
        Box::pin(async move {
            // Validate every query type up front (before the empty-container
            // shortcut and per-node filtering) so an unsupported variant yields
            // HTTP 400 regardless of node count or query order — parity with
            // Python tiled, which raises UnsupportedQueryType at query dispatch.
            // `?` converts UnsupportedQuery → TiledError::UnsupportedQuery.
            for q in queries {
                ensure_supported(q)?;
            }
            if queries.is_empty() {
                return Ok(self.mapping.keys().cloned().collect());
            }
            Ok(self
                .mapping
                .iter()
                .filter(|(_, adapter)| queries.iter().all(|q| matches_query(adapter, q)))
                .map(|(k, _)| k.clone())
                .collect())
        })
    }
}

/// Single source of truth for which query variants this in-memory adapter can
/// evaluate. `search` gates every query through this before calling
/// `matches_query`. The map adapter evaluates every metadata predicate, plus
/// `AccessBlobFilter` (via `include_untagged`); only the tree-position filters
/// `Lookup`/`KeysFilter` have no in-memory evaluation here (the search
/// endpoint resolves `Lookup` by direct key lookup; `KeysFilter` is not
/// implemented). Those become `UnsupportedQuery` → HTTP 400 (parity: Python's
/// `MapAdapter` registers neither `KeyLookup`) rather than silently passing
/// every node through.
fn ensure_supported(query: &Query) -> Result<(), UnsupportedQuery> {
    match query {
        Query::Lookup(_) | Query::KeysFilter(_) => {
            Err(UnsupportedQuery(query.type_name().to_string()))
        }
        _ => Ok(()),
    }
}

/// Check if an adapter matches a single query against its metadata.
fn matches_query(adapter: &AnyAdapter, query: &Query) -> bool {
    let meta = adapter.metadata();
    match query {
        Query::FullText(ft) => meta.to_string().contains(&ft.text),
        Query::Eq(eq) => meta.get(&eq.key).is_some_and(|v| v == &eq.value),
        Query::NotEq(neq) => meta.get(&neq.key).is_none_or(|v| v != &neq.value),
        Query::KeyPresent(kp) => meta.get(&kp.key).is_some() == kp.exists,
        Query::Contains(c) => meta
            .get(&c.key)
            .and_then(|v| v.as_array())
            .is_some_and(|arr| arr.contains(&c.value)),
        Query::StructureFamily(sf) => adapter.structure_family() == sf.value,
        Query::Comparison(c) => meta
            .get(&c.key)
            .is_some_and(|v| compare_json(v, &c.value, c.operator)),
        Query::In(q) => meta
            .get(&q.key)
            .is_some_and(|v| q.value.iter().any(|x| x == v)),
        Query::NotIn(q) => meta
            .get(&q.key)
            .is_none_or(|v| !q.value.iter().any(|x| x == v)),
        Query::Like(l) => {
            let regex_pat = sql_like_to_regex(&l.pattern);
            regex::Regex::new(&regex_pat)
                .ok()
                .and_then(|re| {
                    meta.get(&l.key)
                        .and_then(|v| v.as_str())
                        .map(|s| re.is_match(s))
                })
                .unwrap_or(false)
        }
        Query::Regex(r) => regex::RegexBuilder::new(&r.pattern)
            .case_insensitive(!r.case_sensitive)
            .build()
            .ok()
            .and_then(|re| {
                meta.get(&r.key)
                    .and_then(|v| v.as_str())
                    .map(|s| re.is_match(s))
            })
            .unwrap_or(false),
        Query::Specs(s) => {
            let names: std::collections::HashSet<_> =
                adapter.specs().iter().map(|sp| sp.name.as_str()).collect();
            s.include.iter().all(|n| names.contains(n.as_str()))
                && !s.exclude.iter().any(|n| names.contains(n.as_str()))
        }
        // Lookup/KeysFilter have no in-memory evaluation here; `search`
        // rejects them up front via `ensure_supported` (→ HTTP 400), so this
        // arm is unreachable through the normal path. Keep it as a defensive
        // default for any direct caller.
        Query::Lookup(_) | Query::KeysFilter(_) => true,
        // AccessBlobFilter: in-memory nodes have no access_blob (untagged),
        // so they match only when include_untagged is true (fail-closed on
        // tagged filters that cannot be evaluated).
        Query::AccessBlobFilter(f) => f.include_untagged,
    }
}

fn compare_json(
    left: &serde_json::Value,
    right: &serde_json::Value,
    op: tiled_core::queries::Operator,
) -> bool {
    use std::cmp::Ordering;
    use tiled_core::queries::Operator;
    let cmp = match (left.as_f64(), right.as_f64()) {
        (Some(a), Some(b)) => a.partial_cmp(&b),
        _ => match (left.as_str(), right.as_str()) {
            (Some(a), Some(b)) => Some(a.cmp(b)),
            _ => None,
        },
    };
    let Some(cmp) = cmp else { return false };
    matches!(
        (op, cmp),
        (Operator::Lt, Ordering::Less)
            | (Operator::Gt, Ordering::Greater)
            | (Operator::Le, Ordering::Less | Ordering::Equal)
            | (Operator::Ge, Ordering::Greater | Ordering::Equal)
    )
}

fn sql_like_to_regex(pat: &str) -> String {
    let mut out = String::from("^");
    for ch in pat.chars() {
        match ch {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            c => out.push_str(&regex::escape(&c.to_string())),
        }
    }
    out.push('$');
    out
}

#[cfg(test)]
mod tests {
    use tiled_core::queries::NotIn as NotInQ;

    use super::*;

    fn leaf(meta: serde_json::Value) -> AnyAdapter {
        AnyAdapter::Container(Arc::new(MapAdapter::new(IndexMap::new(), meta, vec![])))
    }

    // NotIn semantics: an adapter whose key is absent must be INCLUDED
    // (mirrors MongoDB $nin and SQL "IS NULL OR NOT IN (...)").

    #[test]
    fn notin_includes_adapter_missing_key() {
        let a = leaf(serde_json::json!({"color": "red"}));
        let q = Query::NotIn(NotInQ {
            key: "shape".into(),
            value: vec![serde_json::json!("circle"), serde_json::json!("square")],
        });
        assert!(
            matches_query(&a, &q),
            "NotIn must include adapters that lack the queried key (missing ≠ excluded)"
        );
    }

    #[test]
    fn notin_excludes_adapter_with_matching_value() {
        let a = leaf(serde_json::json!({"shape": "circle"}));
        let q = Query::NotIn(NotInQ {
            key: "shape".into(),
            value: vec![serde_json::json!("circle"), serde_json::json!("square")],
        });
        assert!(
            !matches_query(&a, &q),
            "NotIn must exclude adapters whose key value is in the list"
        );
    }

    // An unsupported query variant must surface as UnsupportedQuery
    // (→ HTTP 400), not silently pass every node through.

    #[tokio::test]
    async fn search_unsupported_variant_errors() {
        let mut mapping = IndexMap::new();
        mapping.insert(
            "a".to_string(),
            AnyAdapter::Container(Arc::new(MapAdapter::new(
                IndexMap::new(),
                serde_json::json!({}),
                vec![],
            ))),
        );
        let map = MapAdapter::new(mapping, serde_json::json!({}), vec![]);
        let q = Query::Lookup(tiled_core::queries::KeyLookup { key: "x".into() });
        let err = map.search(std::slice::from_ref(&q)).await.unwrap_err();
        assert!(
            matches!(err, tiled_core::error::TiledError::UnsupportedQuery(ref s) if s == "The query type 'KeyLookup' is not supported on this node."),
            "expected UnsupportedQuery for KeyLookup, got {err:?}"
        );
    }

    #[tokio::test]
    async fn search_empty_container_still_errors_on_unsupported() {
        // Up-front validation must fire even with no nodes to filter.
        let map = MapAdapter::new(IndexMap::new(), serde_json::json!({}), vec![]);
        let q = Query::KeysFilter(tiled_core::queries::KeysFilter {
            keys: vec!["k".into()],
        });
        let err = map.search(std::slice::from_ref(&q)).await.unwrap_err();
        assert!(
            matches!(err, tiled_core::error::TiledError::UnsupportedQuery(ref s) if s == "The query type 'KeysFilter' is not supported on this node."),
            "expected UnsupportedQuery for KeysFilter, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_map_adapter_basic() {
        let mapping = IndexMap::new();
        let adapter = MapAdapter::new(mapping, serde_json::json!({}), vec![]);
        assert_eq!(adapter.structure_family(), StructureFamily::Container);
        assert_eq!(adapter.len().await.unwrap(), 0);
        assert!(adapter.is_empty().await.unwrap());
    }

    #[test]
    fn test_map_adapter_with_children() {
        let mapping = IndexMap::new();
        let adapter = MapAdapter::new(mapping, serde_json::json!({"name": "root"}), vec![]);
        assert_eq!(adapter.metadata()["name"], "root");
    }

    #[tokio::test]
    async fn test_items_range() {
        let mut mapping = IndexMap::new();
        for i in 0..10 {
            let child = MapAdapter::new(IndexMap::new(), serde_json::json!({}), vec![]);
            mapping.insert(format!("item_{i}"), AnyAdapter::Container(Arc::new(child)));
        }
        let adapter = MapAdapter::new(mapping, serde_json::json!({}), vec![]);
        assert_eq!(adapter.len().await.unwrap(), 10);

        let page: Vec<&str> = adapter.items_range(2, 3).map(|(k, _)| k).collect();
        assert_eq!(page, vec!["item_2", "item_3", "item_4"]);

        let page: Vec<&str> = adapter.items_range(8, 5).map(|(k, _)| k).collect();
        assert_eq!(page, vec!["item_8", "item_9"]);
    }

    #[tokio::test]
    async fn test_structure_reports_keys() {
        let mut mapping = IndexMap::new();
        mapping.insert(
            "a".to_string(),
            AnyAdapter::Container(Arc::new(MapAdapter::new(
                IndexMap::new(),
                serde_json::json!({}),
                vec![],
            ))),
        );
        let adapter = MapAdapter::new(mapping, serde_json::json!({}), vec![]);

        // structure() now returns an owned ContainerStructure (the OnceLock
        // borrow cache is gone with the async trait); it must report the keys.
        let s = adapter.structure().await.unwrap();
        assert_eq!(s.keys, vec!["a"]);
    }
}
