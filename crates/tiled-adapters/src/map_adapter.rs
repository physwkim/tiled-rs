//! In-memory container adapter backed by an IndexMap.
//!
//! Corresponds to `tiled/adapters/mapping.py:MapAdapter`.

use std::sync::{Arc, OnceLock};

use indexmap::IndexMap;

use tiled_core::adapters::{AnyAdapter, BaseAdapter, ContainerAdapter};
use tiled_core::queries::Query;
use tiled_core::schemas::{SortDirection, SortingItem};
use tiled_core::structures::{ContainerStructure, Spec, StructureFamily};

/// An in-memory container adapter using an ordered map.
pub struct MapAdapter {
    mapping: Arc<IndexMap<String, AnyAdapter>>,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    sorting: Vec<SortingItem>,
    must_revalidate: bool,
    /// Cached structure, lazily initialized.
    structure_cache: OnceLock<ContainerStructure>,
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
            structure_cache: OnceLock::new(),
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
    fn structure(&self) -> &ContainerStructure {
        self.structure_cache.get_or_init(|| ContainerStructure {
            keys: self.mapping.keys().cloned().collect(),
        })
    }

    #[inline]
    fn get(&self, key: &str) -> Option<&AnyAdapter> {
        self.mapping.get(key)
    }

    fn keys(&self) -> Vec<String> {
        self.mapping.keys().cloned().collect()
    }

    #[inline]
    fn len(&self) -> usize {
        self.mapping.len()
    }

    fn search(&self, queries: &[Query]) -> Vec<String> {
        if queries.is_empty() {
            return self.keys();
        }
        self.mapping
            .iter()
            .filter(|(_, adapter)| queries.iter().all(|q| matches_query(adapter, q)))
            .map(|(k, _)| k.clone())
            .collect()
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
            .is_some_and(|v| !q.value.iter().any(|x| x == v)),
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
        // Lookup is resolved at the search endpoint via direct key lookup;
        // KeysFilter the in-memory adapter does not implement — pass through.
        // AccessBlobFilter: in-memory nodes have no access_blob (untagged),
        // so they match only when include_untagged is true (fail-closed on
        // tagged filters that cannot be evaluated).
        Query::Lookup(_) | Query::KeysFilter(_) => true,
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
    use super::*;

    #[test]
    fn test_map_adapter_basic() {
        let mapping = IndexMap::new();
        let adapter = MapAdapter::new(mapping, serde_json::json!({}), vec![]);
        assert_eq!(adapter.structure_family(), StructureFamily::Container);
        assert_eq!(adapter.len(), 0);
        assert!(adapter.is_empty());
    }

    #[test]
    fn test_map_adapter_with_children() {
        let mapping = IndexMap::new();
        let adapter = MapAdapter::new(mapping, serde_json::json!({"name": "root"}), vec![]);
        assert_eq!(adapter.metadata()["name"], "root");
    }

    #[test]
    fn test_items_range() {
        let mut mapping = IndexMap::new();
        for i in 0..10 {
            let child = MapAdapter::new(IndexMap::new(), serde_json::json!({}), vec![]);
            mapping.insert(format!("item_{i}"), AnyAdapter::Container(Arc::new(child)));
        }
        let adapter = MapAdapter::new(mapping, serde_json::json!({}), vec![]);
        assert_eq!(adapter.len(), 10);

        let page: Vec<&str> = adapter.items_range(2, 3).map(|(k, _)| k).collect();
        assert_eq!(page, vec!["item_2", "item_3", "item_4"]);

        let page: Vec<&str> = adapter.items_range(8, 5).map(|(k, _)| k).collect();
        assert_eq!(page, vec!["item_8", "item_9"]);
    }

    #[test]
    fn test_structure_cached() {
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

        let s1 = adapter.structure();
        let s2 = adapter.structure();
        // Same pointer — OnceLock caches it.
        assert!(std::ptr::eq(s1, s2));
        assert_eq!(s1.keys, vec!["a"]);
    }
}
