//! `CatalogAdapter` — exposes the SQL catalog as a [`ContainerAdapter`].
//!
//! Each instance represents one container level: the root has `parent_id =
//! None`; descendants point to their owning row. Children are fetched
//! eagerly on first access (matching `tiled_mongo::MongoCatalog`) so the
//! existing sync trait can return `&AnyAdapter`. Migrating to lazy lookup
//! would require widening the trait — out of scope for the catalog phase.
//!
//! The data-source loader picks an adapter shape based on
//! `Node.structure_family`. Leaf families (Array/Table/Sparse/Awkward) are
//! materialised through the registered file-system adapters in the
//! `tiled-adapters` crate. Until those are wired, leaves fall back to a
//! `PlaceholderArray`/`PlaceholderTable`/etc. that surfaces the structure
//! but errors on `read()` — Phase C replaces that.

use std::sync::{Arc, OnceLock};

use indexmap::IndexMap;

use tiled_core::adapters::{AnyAdapter, BaseAdapter, ContainerAdapter};
use tiled_core::queries::{Query, UnsupportedQuery};
use tiled_core::structures::{ContainerStructure, Spec, StructureFamily};

use crate::db::Catalog;
use crate::error::CatalogError;
use crate::orm::Node;

/// A container view rooted at a given node (or the catalog root).
pub struct CatalogAdapter {
    catalog: Catalog,
    node_id: Option<i64>,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    /// Children keyed by name; each entry carries the adapter and the raw
    /// `access_blob` JSON so that `AccessBlobFilter` queries can be
    /// evaluated in-memory without a second round-trip to the DB.
    children: OnceLock<IndexMap<String, (AnyAdapter, serde_json::Value)>>,
    structure_cache: OnceLock<ContainerStructure>,
    /// Resolver that turns a leaf `Node` into a concrete adapter (Array,
    /// Table, ...). Phase C wires the real implementations; today we ship
    /// a placeholder so the catalog tree is still walkable end-to-end.
    leaf_resolver: Arc<dyn LeafResolver>,
}

/// Strategy that produces a leaf adapter from a node row + its data
/// sources. Different deployments inject different resolvers (file-system,
/// blob, database).
pub trait LeafResolver: Send + Sync {
    fn resolve(
        &self,
        catalog: &Catalog,
        node: &Node,
    ) -> std::result::Result<AnyAdapter, CatalogError>;
}

impl CatalogAdapter {
    /// Build the root adapter — the public constructor used by the server.
    pub fn root(catalog: Catalog, leaf_resolver: Arc<dyn LeafResolver>) -> Self {
        Self {
            catalog,
            node_id: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
            specs: vec![Spec::with_version("CatalogOfBlueskyRuns", "1")],
            children: OnceLock::new(),
            structure_cache: OnceLock::new(),
            leaf_resolver,
        }
    }

    fn child_container(&self, node: &Node) -> Self {
        Self {
            catalog: self.catalog.clone(),
            node_id: Some(node.id),
            metadata: node.metadata.clone(),
            specs: parse_specs(&node.specs),
            children: OnceLock::new(),
            structure_cache: OnceLock::new(),
            leaf_resolver: self.leaf_resolver.clone(),
        }
    }

    fn load_children(&self) -> &IndexMap<String, (AnyAdapter, serde_json::Value)> {
        self.children.get_or_init(|| {
            // We're called from sync trait methods. The router invokes
            // those inside `spawn_blocking`, so it's safe to dive back into
            // the async runtime here via `Handle::block_on`. Outside that
            // context `try_current()` returns Err — surface an empty map
            // instead of panicking, since panicking the trait method
            // would crash a request handler.
            let handle = match tokio::runtime::Handle::try_current() {
                Ok(h) => h,
                Err(_) => {
                    tracing::warn!(
                        target: "tiled.catalog",
                        "CatalogAdapter::load_children outside async runtime; returning empty"
                    );
                    return IndexMap::new();
                }
            };
            let mut map = IndexMap::new();
            // Bound the eager fetch — million-row catalogs need pagination
            // wired through the trait, but until we widen
            // ContainerAdapter::get to be async we keep this cap explicit.
            const PAGE: i64 = 10_000;
            let mut offset: i64 = 0;
            loop {
                let nodes =
                    match handle.block_on(self.catalog.list_children(self.node_id, offset, PAGE)) {
                        Ok(n) => n,
                        Err(e) => {
                            tracing::error!(target: "tiled.catalog", "list_children failed: {e}");
                            break;
                        }
                    };
                if nodes.is_empty() {
                    break;
                }
                for node in &nodes {
                    let adapter = if matches!(node.structure_family.as_str(), "container") {
                        AnyAdapter::Container(Arc::new(self.child_container(node)))
                    } else {
                        match self.leaf_resolver.resolve(&self.catalog, node) {
                            Ok(a) => a,
                            Err(e) => {
                                tracing::warn!(
                                    target: "tiled.catalog",
                                    "leaf resolver failed for node {}: {e}", node.key,
                                );
                                continue;
                            }
                        }
                    };
                    map.insert(node.key.clone(), (adapter, node.access_blob.clone()));
                }
                if nodes.len() < PAGE as usize {
                    break;
                }
                offset += nodes.len() as i64;
            }
            map
        })
    }

    /// Direct read access to the underlying [`Catalog`] (write API,
    /// bypassing the cached adapter view).
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Forget the cached children. Call after a write so the next read
    /// observes the new tree state. Cheap — just resets the OnceLock.
    pub fn invalidate(&mut self) {
        self.children = OnceLock::new();
        self.structure_cache = OnceLock::new();
    }
}

impl BaseAdapter for CatalogAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Container
    }

    fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }

    fn specs(&self) -> &[Spec] {
        &self.specs
    }
}

impl ContainerAdapter for CatalogAdapter {
    fn structure(&self) -> &ContainerStructure {
        self.structure_cache.get_or_init(|| ContainerStructure {
            keys: self.load_children().keys().cloned().collect(),
        })
    }

    fn get(&self, key: &str) -> Option<&AnyAdapter> {
        self.load_children().get(key).map(|(a, _)| a)
    }

    fn keys(&self) -> Vec<String> {
        self.load_children().keys().cloned().collect()
    }

    fn len(&self) -> usize {
        self.load_children().len()
    }

    fn search(&self, queries: &[Query]) -> Result<Vec<String>, UnsupportedQuery> {
        // Phase A4 will translate queries to SQL; for now apply Python
        // tiled's "match against in-memory metadata" fallback so the
        // existing search endpoint still returns sensible results.
        //
        // Validate every query type up front (before the empty-container
        // shortcut and before per-node filtering) so an unsupported variant
        // yields HTTP 400 regardless of node count or query order — matching
        // Python tiled, which raises UnsupportedQueryType at query dispatch.
        for q in queries {
            ensure_supported(q)?;
        }
        if queries.is_empty() {
            return Ok(self.keys());
        }
        Ok(self
            .load_children()
            .iter()
            .filter(|(_, (adapter, access_blob))| {
                queries
                    .iter()
                    .all(|q| matches_query(adapter, access_blob, q))
            })
            .map(|(k, _)| k.clone())
            .collect())
    }
}

/// Single source of truth for which query variants this in-memory screening
/// path can evaluate. `search` gates every query through this before calling
/// `matches_query`, so the unsupported arms of `matches_query` are never
/// reached at runtime. Variants this path cannot evaluate become
/// `UnsupportedQuery` → HTTP 400 (parity: Python `UnsupportedQueryType`).
fn ensure_supported(query: &Query) -> Result<(), UnsupportedQuery> {
    use Query::*;
    match query {
        // Metadata predicates this in-memory path implements, plus the
        // tree-position filters (Lookup/KeysFilter) that the authoritative SQL
        // path resolves before this screening pass runs.
        FullText(_) | Eq(_) | NotEq(_) | KeyPresent(_) | StructureFamily(_)
        | AccessBlobFilter(_) | Lookup(_) | KeysFilter(_) => Ok(()),
        // In, NotIn, Comparison, Contains, Like, Regex, Specs: metadata
        // predicates not implemented in-memory. Surface as 400 rather than
        // silently returning a filtered subset.
        other => Err(UnsupportedQuery(other.type_name().to_string())),
    }
}

/// Evaluate one query filter against an adapter's in-memory state.
///
/// This path is called only from `CatalogAdapter::search`, whose only
/// reachable callers pass an `AccessBlobFilter`-only query list for
/// access-control screening.  The SQL path (`Catalog::search_children`)
/// is the authoritative evaluator for all user-visible queries.
fn matches_query(adapter: &AnyAdapter, access_blob: &serde_json::Value, query: &Query) -> bool {
    use Query::*;
    let meta = adapter.metadata();
    match query {
        FullText(q) => meta.to_string().contains(&q.text),
        Eq(eq) => meta.get(&eq.key).is_some_and(|v| v == &eq.value),
        NotEq(neq) => meta.get(&neq.key).is_none_or(|v| v != &neq.value),
        KeyPresent(kp) => meta.get(&kp.key).is_some() == kp.exists,
        StructureFamily(sf) => adapter.structure_family() == sf.value,
        AccessBlobFilter(f) => matches_access_blob_filter(access_blob, f),
        // Lookup and KeysFilter filter by node key (tree position), not by
        // metadata — they cannot be evaluated against the in-memory adapter
        // representation.  Pass them through so that an AccessBlobFilter +
        // Lookup combination (if ever issued) does not accidentally exclude
        // every node before the SQL path resolves the key constraint.
        Lookup(_) | KeysFilter(_) => true,
        // All remaining variants (In, NotIn, Comparison, Contains, Like,
        // Regex, Specs) express metadata predicates that this in-memory path
        // does not implement. `search` rejects them up front via
        // `ensure_supported` (→ HTTP 400, parity with UnsupportedQueryType),
        // so this arm is unreachable through the normal path; keep it as a
        // fail-closed default for any direct caller.
        _ => false,
    }
}

fn matches_access_blob_filter(
    access_blob: &serde_json::Value,
    f: &tiled_core::queries::AccessBlobFilter,
) -> bool {
    // Empty filter → deny all (mirrors push_access_blob_filter "1 = 0").
    if f.user_id.is_none() && f.tags.is_empty() && !f.include_untagged {
        return false;
    }
    let node_tags: Vec<&str> = access_blob
        .get("tags")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    // include_untagged: rows with absent/empty tags AND no `user` key are
    // genuinely public. A user-owned blob `{"user": id}` carries no `tags`
    // key but must NOT be treated as public here — it reaches the caller only
    // through the user-ownership arm below, so it never leaks to non-owners.
    if f.include_untagged && node_tags.is_empty() && access_blob.get("user").is_none() {
        return true;
    }
    // Tag intersection.
    if !f.tags.is_empty() && node_tags.iter().any(|t| f.tags.iter().any(|ft| ft == t)) {
        return true;
    }
    // User ownership.
    if let Some(ref uid) = f.user_id
        && access_blob.get("user").and_then(|v| v.as_str()) == Some(uid.as_str())
    {
        return true;
    }
    false
}

fn parse_specs(value: &serde_json::Value) -> Vec<Spec> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    if let Some(s) = v.as_str() {
                        Some(Spec::new(s))
                    } else {
                        let name = v.get("name").and_then(|n| n.as_str())?;
                        let version = v.get("version").and_then(|n| n.as_str());
                        match version {
                            Some(ver) => Some(Spec::with_version(name, ver)),
                            None => Some(Spec::new(name)),
                        }
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Minimal placeholder leaf resolver — returns an error on every leaf so
/// callers know to register a real resolver (Phase C wires the file-system
/// adapters). Used by tests.
pub struct UnresolvedLeaf;

impl LeafResolver for UnresolvedLeaf {
    fn resolve(
        &self,
        _catalog: &Catalog,
        node: &Node,
    ) -> std::result::Result<AnyAdapter, CatalogError> {
        Err(CatalogError::Validation(format!(
            "no leaf resolver registered for structure_family={} (node {})",
            node.structure_family, node.key
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use tiled_core::adapters::{AnyAdapter, BaseAdapter, ContainerAdapter};
    use tiled_core::queries::{
        AccessBlobFilter, Eq as EqQ, In as InQ, KeyLookup, KeysFilter, NotIn as NotInQ, Query,
    };
    use tiled_core::structures::{ContainerStructure, Spec, StructureFamily};

    use super::{ensure_supported, matches_query};

    struct StubContainer {
        metadata: serde_json::Value,
        structure: ContainerStructure,
    }

    impl BaseAdapter for StubContainer {
        fn structure_family(&self) -> StructureFamily {
            StructureFamily::Container
        }
        fn metadata(&self) -> &serde_json::Value {
            &self.metadata
        }
        fn specs(&self) -> &[Spec] {
            &[]
        }
    }

    impl ContainerAdapter for StubContainer {
        fn structure(&self) -> &ContainerStructure {
            &self.structure
        }
        fn get(&self, _key: &str) -> Option<&AnyAdapter> {
            None
        }
        fn keys(&self) -> Vec<String> {
            vec![]
        }
        fn len(&self) -> usize {
            0
        }
    }

    fn adapter(meta: serde_json::Value) -> AnyAdapter {
        AnyAdapter::Container(Arc::new(StubContainer {
            metadata: meta,
            structure: ContainerStructure { keys: vec![] },
        }))
    }

    // L-1: unsupported metadata-predicate variants must surface as
    // UnsupportedQuery (→ HTTP 400), not silently filter to a subset.

    #[test]
    fn unsupported_in_returns_error() {
        let q = Query::In(InQ {
            key: "count".into(),
            value: vec![json!(5)],
        });
        let err = ensure_supported(&q).unwrap_err();
        assert_eq!(
            err.0, "In",
            "In is unsupported in-memory and must error, not silently pass"
        );
    }

    #[test]
    fn unsupported_not_in_returns_error() {
        let q = Query::NotIn(NotInQ {
            key: "tag".into(),
            value: vec![json!("y")],
        });
        assert_eq!(ensure_supported(&q).unwrap_err().0, "NotIn");
    }

    // Lookup / KeysFilter must still pass through (genuinely inapplicable
    // in-memory; the SQL path owns key-based filtering).

    #[test]
    fn lookup_passes_through() {
        let a = adapter(json!({}));
        let blob = json!({});
        let q = Query::Lookup(KeyLookup {
            key: "anything".into(),
        });
        // Lookup stays supported here (the SQL path resolves keys), unlike the
        // map/mongo adapters where it has no pre-filter and errors.
        assert!(ensure_supported(&q).is_ok(), "Lookup must stay supported");
        assert!(matches_query(&a, &blob, &q), "Lookup must pass through");
    }

    #[test]
    fn keys_filter_passes_through() {
        let a = adapter(json!({}));
        let blob = json!({});
        let q = Query::KeysFilter(KeysFilter {
            keys: vec!["k".into()],
        });
        assert!(
            ensure_supported(&q).is_ok(),
            "KeysFilter must stay supported"
        );
        assert!(matches_query(&a, &blob, &q), "KeysFilter must pass through");
    }

    // AccessBlobFilter must still work (the reachable use-case).

    #[test]
    fn access_blob_filter_matches_tagged_node() {
        let a = adapter(json!({}));
        let blob = json!({"tags": ["team_a"]});
        let q = Query::AccessBlobFilter(AccessBlobFilter {
            tags: vec!["team_a".into()],
            ..Default::default()
        });
        assert!(matches_query(&a, &blob, &q));
    }

    #[test]
    fn access_blob_filter_denies_unmatched_node() {
        let a = adapter(json!({}));
        let blob = json!({"tags": ["team_b"]});
        let q = Query::AccessBlobFilter(AccessBlobFilter {
            tags: vec!["team_a".into()],
            ..Default::default()
        });
        assert!(!matches_query(&a, &blob, &q));
    }

    /// Regression (fail-open leak): include_untagged must NOT match a
    /// user-owned blob `{"user": id}` (which has no `tags` key). Such a node
    /// is reachable only by its owner via the user-ownership arm.
    #[test]
    fn access_blob_filter_untagged_excludes_user_owned() {
        let a = adapter(json!({}));
        let blob = json!({"user": "bob-uuid"});
        // Anonymous-style filter: no user_id, no tags, only include_untagged.
        let anon = Query::AccessBlobFilter(AccessBlobFilter {
            include_untagged: true,
            ..Default::default()
        });
        assert!(
            !matches_query(&a, &blob, &anon),
            "user-owned node must not be visible via the untagged-public arm"
        );
        // Cross-user filter: alice's grant must not surface bob's owned node.
        let alice = Query::AccessBlobFilter(AccessBlobFilter {
            user_id: Some("alice-uuid".into()),
            include_untagged: true,
            ..Default::default()
        });
        assert!(
            !matches_query(&a, &blob, &alice),
            "another user's owned node must not be visible to alice"
        );
        // The owner still sees their own node via the user arm.
        let owner = Query::AccessBlobFilter(AccessBlobFilter {
            user_id: Some("bob-uuid".into()),
            include_untagged: true,
            ..Default::default()
        });
        assert!(
            matches_query(&a, &blob, &owner),
            "owner must still see their own node"
        );
    }

    // Supported metadata variants (Eq, NotEq) must continue to work.

    #[test]
    fn eq_matches_correct_value() {
        let a = adapter(json!({"x": 1}));
        let blob = json!({});
        assert!(matches_query(
            &a,
            &blob,
            &Query::Eq(EqQ {
                key: "x".into(),
                value: json!(1)
            })
        ));
        assert!(!matches_query(
            &a,
            &blob,
            &Query::Eq(EqQ {
                key: "x".into(),
                value: json!(2)
            })
        ));
    }
}
