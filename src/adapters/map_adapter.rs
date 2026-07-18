//! In-memory container adapter backed by an IndexMap.
//!
//! Corresponds to `tiled/adapters/mapping.py:MapAdapter`.

use std::sync::Arc;

use indexmap::IndexMap;

use crate::core::adapters::{AnyAdapter, BaseAdapter, BoxFuture, ContainerAdapter};
use crate::core::queries::{Query, UnsupportedQuery};
use crate::core::schemas::{SortDirection, SortingItem};
use crate::core::structures::{ContainerStructure, Spec, StructureFamily};

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
    fn structure(&self) -> BoxFuture<'_, crate::core::error::Result<ContainerStructure>> {
        Box::pin(async move {
            Ok(ContainerStructure {
                keys: self.mapping.keys().cloned().collect(),
            })
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, crate::core::error::Result<Option<AnyAdapter>>> {
        // In-memory: an owned clone is a cheap `Arc` refcount bump.
        Box::pin(async move { Ok(self.mapping.get(key).cloned()) })
    }

    fn keys(&self) -> BoxFuture<'_, crate::core::error::Result<Vec<String>>> {
        Box::pin(async move { Ok(self.mapping.keys().cloned().collect()) })
    }

    fn len(&self) -> BoxFuture<'_, crate::core::error::Result<usize>> {
        Box::pin(async move { Ok(self.mapping.len()) })
    }

    fn search<'a>(
        &'a self,
        queries: &'a [Query],
    ) -> BoxFuture<'a, crate::core::error::Result<Vec<String>>> {
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
                .filter(|(key, adapter)| {
                    queries.iter().all(|q| match q {
                        // Tree-position filter: evaluated here where the node
                        // key is in scope (parity with Python `keys_filter`).
                        // An empty key list matches nothing.
                        Query::KeysFilter(kf) => kf.keys.iter().any(|k| k == *key),
                        _ => matches_query(adapter, q),
                    })
                })
                .map(|(k, _)| k.clone())
                .collect())
        })
    }

    /// Order matched keys by child metadata, mirroring Python `MapAdapter.sort`
    /// (adapters/mapping.py:353). The sort terms form a stable chain applied
    /// right-to-left: the `"_"` sentinel re-sorts nothing — it only reverses the
    /// running order when its direction is descending; every other key orders by
    /// the literal top-level metadata value at `key` (`metadata().get(key)`, not
    /// a dotted descent — matching upstream), with a missing value sorted last
    /// (`_HIGH_SORTER`). A key dropped between `search` and here also sorts as
    /// missing metadata. Reads only in-memory `metadata()`, so the extra passes
    /// over the matched keys stay cheap.
    fn sort_matched_keys(
        &self,
        mut keys: Vec<String>,
        sorting: &[(String, SortDirection)],
    ) -> Vec<String> {
        for (key, direction) in sorting.iter().rev() {
            if key != "_" {
                keys.sort_by(|a, b| {
                    let va = self.mapping.get(a).and_then(|ad| ad.metadata().get(key));
                    let vb = self.mapping.get(b).and_then(|ad| ad.metadata().get(key));
                    json_sort_cmp(va, vb)
                });
            }
            if *direction == SortDirection::Descending {
                keys.reverse();
            }
        }
        keys
    }
}

/// Single source of truth for which query variants this in-memory adapter can
/// evaluate. `search` gates every query through this before filtering. The map
/// adapter evaluates every metadata predicate, `KeysFilter` (by node key, in
/// `search`), and `AccessBlobFilter` (via `include_untagged`). Only `Lookup`
/// (`KeyLookup`) has no in-memory evaluation — Python's `MapAdapter` registers
/// no `KeyLookup` handler either (the search endpoint resolves it by direct key
/// lookup) — so it becomes `UnsupportedQuery` → HTTP 400 rather than silently
/// passing every node through.
fn ensure_supported(query: &Query) -> Result<(), UnsupportedQuery> {
    match query {
        Query::Lookup(_) => Err(UnsupportedQuery(query.type_name().to_string())),
        _ => Ok(()),
    }
}

/// Resolve a (possibly dotted) metadata key to its JSON value, walking nested
/// objects: `"a.b"` descends `meta["a"]["b"]`. Returns `None` when any segment
/// is absent (or an intermediate isn't an object). Mirrors Python
/// `iter_child_metadata`, which splits the key on "." and only yields a child
/// when the whole path resolves.
fn nested_get<'a>(meta: &'a serde_json::Value, key: &str) -> Option<&'a serde_json::Value> {
    let mut cur = meta;
    for seg in key.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// Whole-word, case-insensitive full-text match over the metadata's string
/// values, mirroring Python `full_text_search` + `walk_string_values`: collect
/// the lower-cased whitespace-delimited words of every string value (keys and
/// non-string scalars are ignored; a list contributes only its direct string
/// elements) and match if any query word appears among them. The query text is
/// split but NOT lower-cased — faithful to Python's `set(text.split())` vs
/// `s.lower().split()` — so an upper-cased query word will not match.
fn full_text_matches(meta: &serde_json::Value, text: &str) -> bool {
    let mut words: std::collections::HashSet<String> = std::collections::HashSet::new();
    collect_string_words(meta, &mut words);
    text.split_whitespace().any(|qw| words.contains(qw))
}

fn collect_string_words(v: &serde_json::Value, out: &mut std::collections::HashSet<String>) {
    match v {
        serde_json::Value::String(s) => {
            out.extend(s.to_lowercase().split_whitespace().map(str::to_string));
        }
        serde_json::Value::Object(map) => {
            for val in map.values() {
                collect_string_words(val, out);
            }
        }
        serde_json::Value::Array(arr) => {
            for item in arr {
                if let serde_json::Value::String(s) = item {
                    out.extend(s.to_lowercase().split_whitespace().map(str::to_string));
                }
            }
        }
        _ => {}
    }
}

/// Check if an adapter matches a single query against its metadata.
fn matches_query(adapter: &AnyAdapter, query: &Query) -> bool {
    let meta = adapter.metadata();
    match query {
        // Word-set match over the string values of the metadata (parity with
        // Python full_text_search + walk_string_values), not a raw substring.
        Query::FullText(ft) => full_text_matches(meta, &ft.text),
        // Metadata predicates address a (possibly dotted) nested path via
        // `nested_get` and, mirroring Python's `iter_child_metadata`, match only
        // when that path resolves — a node missing the key is excluded.
        Query::Eq(eq) => nested_get(meta, &eq.key).is_some_and(|v| v == &eq.value),
        Query::NotEq(neq) => nested_get(meta, &neq.key).is_some_and(|v| v != &neq.value),
        // KeyPresent: presence of the nested path equals the requested flag.
        // (Python's `key_present` handler is itself broken — it ignores `exists`
        // and substring-checks the value against the key — so we implement the
        // intended present/absent semantics rather than copying that bug.)
        Query::KeyPresent(kp) => nested_get(meta, &kp.key).is_some() == kp.exists,
        Query::Contains(c) => nested_get(meta, &c.key)
            .and_then(|v| v.as_array())
            .is_some_and(|arr| arr.contains(&c.value)),
        Query::StructureFamily(sf) => adapter.structure_family() == sf.value,
        Query::Comparison(c) => {
            nested_get(meta, &c.key).is_some_and(|v| compare_json(v, &c.value, c.operator))
        }
        Query::In(q) => nested_get(meta, &q.key).is_some_and(|v| q.value.iter().any(|x| x == v)),
        // NotIn: an empty value list matches everything (Python returns the
        // whole tree); otherwise the path must resolve and its value must be
        // absent from the list (present-only, like the other predicates).
        Query::NotIn(q) => {
            q.value.is_empty()
                || nested_get(meta, &q.key).is_some_and(|v| !q.value.iter().any(|x| x == v))
        }
        Query::Like(l) => {
            let regex_pat = sql_like_to_regex(&l.pattern);
            regex::Regex::new(&regex_pat)
                .ok()
                .and_then(|re| {
                    nested_get(meta, &l.key)
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
                nested_get(meta, &r.key)
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
        // KeysFilter is evaluated in `search` (it needs the node key); Lookup is
        // rejected by `ensure_supported`. Neither reaches here through the normal
        // path — keep a defensive default for any direct caller.
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
    op: crate::core::queries::Operator,
) -> bool {
    use crate::core::queries::Operator;
    use std::cmp::Ordering;
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

/// Total order over an optional metadata value for [`MapAdapter::sort_matched_keys`].
/// `None` (the key is absent from a child's metadata) is greatest, so missing
/// keys sort last in ascending order — the Rust analog of Python's `_HIGH_SORTER`
/// sentinel (adapters/mapping.py:851). Present values order by JSON type rank
/// then by value; unorderable or heterogeneous types (null / array / object, or
/// a number-vs-string pair) compare as their type rank and otherwise Equal, so a
/// stable sort leaves them in input order rather than panicking (Python raises
/// `TypeError` on such a mix — an error path no real sorted listing hits).
fn json_sort_cmp(
    a: Option<&serde_json::Value>,
    b: Option<&serde_json::Value>,
) -> std::cmp::Ordering {
    use serde_json::Value;
    use std::cmp::Ordering;
    fn rank(v: Option<&Value>) -> u8 {
        match v {
            Some(Value::Null) => 0,
            Some(Value::Bool(_)) => 1,
            Some(Value::Number(_)) => 2,
            Some(Value::String(_)) => 3,
            Some(Value::Array(_)) => 4,
            Some(Value::Object(_)) => 5,
            None => 6, // _HIGH_SORTER: an absent key sorts last (ascending).
        }
    }
    let (ra, rb) = (rank(a), rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Some(Value::Bool(x)), Some(Value::Bool(y))) => x.cmp(y),
        // Compare numbers as f64 (same widening `compare_json` uses above).
        (Some(Value::Number(x)), Some(Value::Number(y))) => x
            .as_f64()
            .partial_cmp(&y.as_f64())
            .unwrap_or(Ordering::Equal),
        (Some(Value::String(x)), Some(Value::String(y))) => x.cmp(y),
        _ => Ordering::Equal,
    }
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
    use crate::core::queries::{
        Eq as EqQ, FullText, KeyLookup, KeyPresent, KeysFilter, NotEq as NotEqQ, NotIn as NotInQ,
    };

    use super::*;

    fn leaf(meta: serde_json::Value) -> AnyAdapter {
        AnyAdapter::Container(Arc::new(MapAdapter::new(IndexMap::new(), meta, vec![])))
    }

    // NotIn semantics (parity: Python MapAdapter `notin` via `iter_child_metadata`,
    // which yields only present-path children): an adapter whose key is ABSENT is
    // EXCLUDED. NotIn matches only nodes that have the key with a non-listed value.
    // (This differs from the SQL catalog's `IS NULL OR NOT IN`, which includes
    // missing — each backend matches its own upstream semantics.)

    #[test]
    fn notin_excludes_adapter_missing_key() {
        let a = leaf(serde_json::json!({"color": "red"}));
        let q = Query::NotIn(NotInQ {
            key: "shape".into(),
            value: vec![serde_json::json!("circle"), serde_json::json!("square")],
        });
        assert!(
            !matches_query(&a, &q),
            "NotIn must exclude adapters that lack the queried key (Python present-only)"
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
        let q = Query::Lookup(crate::core::queries::KeyLookup { key: "x".into() });
        let err = map.search(std::slice::from_ref(&q)).await.unwrap_err();
        assert!(
            matches!(err, crate::core::error::TiledError::UnsupportedQuery(ref s) if s == "The query type 'KeyLookup' is not supported on this node."),
            "expected UnsupportedQuery for KeyLookup, got {err:?}"
        );
    }

    #[tokio::test]
    async fn search_empty_container_still_errors_on_unsupported() {
        // Up-front validation must fire even with no nodes to filter. KeyLookup
        // is the only query MapAdapter rejects (Python registers no handler for
        // it); KeysFilter is now supported.
        let map = MapAdapter::new(IndexMap::new(), serde_json::json!({}), vec![]);
        let q = Query::Lookup(KeyLookup { key: "x".into() });
        let err = map.search(std::slice::from_ref(&q)).await.unwrap_err();
        assert!(
            matches!(err, crate::core::error::TiledError::UnsupportedQuery(ref s) if s == "The query type 'KeyLookup' is not supported on this node."),
            "expected UnsupportedQuery for KeyLookup, got {err:?}"
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

    // Parity with Python MapAdapter `iter_child_metadata` (key.split('.')): a
    // dotted key addresses a nested path, not a flat top-level key named "a.b".

    #[test]
    fn eq_dotted_key_addresses_nested_value() {
        let a = leaf(serde_json::json!({"a": {"b": 7}}));
        assert!(matches_query(
            &a,
            &Query::Eq(EqQ {
                key: "a.b".into(),
                value: serde_json::json!(7)
            })
        ));
        assert!(!matches_query(
            &a,
            &Query::Eq(EqQ {
                key: "a.b".into(),
                value: serde_json::json!(8)
            })
        ));
        // Absent nested path → no match (present-only).
        assert!(!matches_query(
            &a,
            &Query::Eq(EqQ {
                key: "a.z".into(),
                value: serde_json::json!(7)
            })
        ));
    }

    #[test]
    fn key_present_dotted_key() {
        let a = leaf(serde_json::json!({"a": {"b": 1}}));
        assert!(matches_query(
            &a,
            &Query::KeyPresent(KeyPresent {
                key: "a.b".into(),
                exists: true
            })
        ));
        assert!(matches_query(
            &a,
            &Query::KeyPresent(KeyPresent {
                key: "a.c".into(),
                exists: false
            })
        ));
    }

    #[test]
    fn not_eq_excludes_missing_key() {
        // Python present-only: a node lacking the key is NOT matched by NotEq.
        let present_diff = leaf(serde_json::json!({"x": 1}));
        let present_eq = leaf(serde_json::json!({"x": 2}));
        let missing = leaf(serde_json::json!({"y": 1}));
        let q = Query::NotEq(NotEqQ {
            key: "x".into(),
            value: serde_json::json!(2),
        });
        assert!(matches_query(&present_diff, &q));
        assert!(!matches_query(&present_eq, &q));
        assert!(
            !matches_query(&missing, &q),
            "NotEq must exclude a node missing the key (Python present-only)"
        );
    }

    #[test]
    fn not_in_empty_list_matches_all() {
        let a = leaf(serde_json::json!({"x": 1}));
        let missing = leaf(serde_json::json!({}));
        let q = Query::NotIn(NotInQ {
            key: "x".into(),
            value: vec![],
        });
        assert!(matches_query(&a, &q));
        assert!(
            matches_query(&missing, &q),
            "an empty NotIn list matches everything (Python returns the whole tree)"
        );
    }

    #[test]
    fn full_text_word_set_not_substring() {
        let a = leaf(serde_json::json!({"material": "Copper Oxide", "n": 5}));
        // Whole word, case-insensitive on the metadata side.
        assert!(matches_query(
            &a,
            &Query::FullText(FullText {
                text: "copper".into()
            })
        ));
        assert!(matches_query(
            &a,
            &Query::FullText(FullText {
                text: "oxide".into()
            })
        ));
        // A token prefix is a word miss (not a substring match).
        assert!(!matches_query(
            &a,
            &Query::FullText(FullText {
                text: "copp".into()
            })
        ));
        // Keys and non-string scalars are not indexed.
        assert!(!matches_query(
            &a,
            &Query::FullText(FullText {
                text: "material".into()
            })
        ));
        assert!(!matches_query(
            &a,
            &Query::FullText(FullText { text: "5".into() })
        ));
        // The query side is not lower-cased (faithful to Python), so an
        // upper-cased query word misses the lower-cased metadata words.
        assert!(!matches_query(
            &a,
            &Query::FullText(FullText {
                text: "Copper".into()
            })
        ));
    }

    #[tokio::test]
    async fn keys_filter_selects_by_node_key() {
        let mut mapping = IndexMap::new();
        mapping.insert("alpha".to_string(), leaf(serde_json::json!({})));
        mapping.insert("beta".to_string(), leaf(serde_json::json!({})));
        mapping.insert("gamma".to_string(), leaf(serde_json::json!({})));
        let map = MapAdapter::new(mapping, serde_json::json!({}), vec![]);

        let q = Query::KeysFilter(KeysFilter {
            keys: vec!["alpha".into(), "gamma".into()],
        });
        let mut hits = map.search(std::slice::from_ref(&q)).await.unwrap();
        hits.sort();
        assert_eq!(hits, vec!["alpha".to_string(), "gamma".to_string()]);

        // An empty key list matches nothing.
        let q = Query::KeysFilter(KeysFilter { keys: vec![] });
        assert!(
            map.search(std::slice::from_ref(&q))
                .await
                .unwrap()
                .is_empty()
        );
    }

    // --- sort_matched_keys (MapAdapter sort; Python adapters/mapping.py:353) ---

    /// Build a MapAdapter from ordered `(key, metadata)` pairs; insertion order
    /// is preserved (IndexMap), matching a Python dict.
    fn map_of(children: &[(&str, serde_json::Value)]) -> MapAdapter {
        let mut mapping = IndexMap::new();
        for (k, meta) in children {
            mapping.insert((*k).to_string(), leaf(meta.clone()));
        }
        MapAdapter::new(mapping, serde_json::json!({}), vec![])
    }

    fn term(key: &str, dir: SortDirection) -> (String, SortDirection) {
        (key.to_string(), dir)
    }

    #[test]
    fn sort_ascending_by_numeric_key_missing_last() {
        let map = map_of(&[
            ("a", serde_json::json!({"n": 3})),
            ("b", serde_json::json!({})), // missing "n" → sorts last
            ("c", serde_json::json!({"n": 1})),
            ("d", serde_json::json!({"n": 2})),
        ]);
        let keys = vec!["a".into(), "b".into(), "c".into(), "d".into()];
        let out = map.sort_matched_keys(keys, &[term("n", SortDirection::Ascending)]);
        assert_eq!(out, vec!["c", "d", "a", "b"]);
    }

    #[test]
    fn sort_descending_reverses_order() {
        let map = map_of(&[
            ("a", serde_json::json!({"n": 3})),
            ("c", serde_json::json!({"n": 1})),
            ("d", serde_json::json!({"n": 2})),
        ]);
        let keys = vec!["a".into(), "c".into(), "d".into()];
        let out = map.sort_matched_keys(keys, &[term("n", SortDirection::Descending)]);
        // Ascending would be c,d,a; descending reverses to a,d,c.
        assert_eq!(out, vec!["a", "d", "c"]);
    }

    #[test]
    fn sort_by_string_key() {
        let map = map_of(&[
            ("x", serde_json::json!({"name": "cherry"})),
            ("y", serde_json::json!({"name": "apple"})),
            ("z", serde_json::json!({"name": "banana"})),
        ]);
        let keys = vec!["x".into(), "y".into(), "z".into()];
        let out = map.sort_matched_keys(keys, &[term("name", SortDirection::Ascending)]);
        assert_eq!(out, vec!["y", "z", "x"]);
    }

    #[test]
    fn sort_underscore_sentinel() {
        let map = map_of(&[
            ("a", serde_json::json!({})),
            ("b", serde_json::json!({})),
            ("c", serde_json::json!({})),
        ]);
        let keys = vec!["a".into(), "b".into(), "c".into()];
        // "_" ascending: leave the given order untouched.
        let out = map.sort_matched_keys(keys.clone(), &[term("_", SortDirection::Ascending)]);
        assert_eq!(out, vec!["a", "b", "c"]);
        // "_" descending: reverse the given order without re-sorting.
        let out = map.sort_matched_keys(keys, &[term("_", SortDirection::Descending)]);
        assert_eq!(out, vec!["c", "b", "a"]);
    }

    #[test]
    fn sort_multi_key_stable_chain() {
        // Primary "grp" ascending, secondary "n" ascending. Terms are applied
        // right-to-left as a stable chain, so ties on "grp" break by "n".
        let map = map_of(&[
            ("a", serde_json::json!({"grp": "y", "n": 2})),
            ("b", serde_json::json!({"grp": "x", "n": 2})),
            ("c", serde_json::json!({"grp": "x", "n": 1})),
            ("d", serde_json::json!({"grp": "y", "n": 1})),
        ]);
        let keys = vec!["a".into(), "b".into(), "c".into(), "d".into()];
        let out = map.sort_matched_keys(
            keys,
            &[
                term("grp", SortDirection::Ascending),
                term("n", SortDirection::Ascending),
            ],
        );
        // grp=x {c(n1), b(n2)}, then grp=y {d(n1), a(n2)}.
        assert_eq!(out, vec!["c", "b", "d", "a"]);
    }

    #[test]
    fn sort_empty_terms_is_no_op() {
        let map = map_of(&[
            ("b", serde_json::json!({"n": 1})),
            ("a", serde_json::json!({"n": 2})),
        ]);
        let keys = vec!["b".into(), "a".into()];
        let out = map.sort_matched_keys(keys.clone(), &[]);
        assert_eq!(out, keys, "no sort terms preserves matched order");
    }

    #[tokio::test]
    async fn search_page_applies_sort_and_windows() {
        // End-to-end through the default search_page: it calls sort_matched_keys
        // then windows by offset/limit. Sort by "n" ascending, take rows [1,3).
        let map = map_of(&[
            ("a", serde_json::json!({"n": 40})),
            ("b", serde_json::json!({"n": 10})),
            ("c", serde_json::json!({"n": 30})),
            ("d", serde_json::json!({"n": 20})),
        ]);
        // Ascending order: b(10), d(20), c(30), a(40). Window [1,3) → d, c.
        let page = map
            .search_page(
                &[],
                &[term("n", SortDirection::Ascending)],
                None,
                1,
                2,
                false,
            )
            .await
            .unwrap();
        let keys: Vec<&str> = page.entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, vec!["d", "c"]);
        assert_eq!(page.total, 4, "total is the full match count, pre-window");
        assert!(page.next_cursor.is_none());
    }
}
