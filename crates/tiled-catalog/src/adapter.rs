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
use tiled_core::queries::Query;
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

    fn search(&self, queries: &[Query]) -> Vec<String> {
        // Phase A4 will translate queries to SQL; for now apply Python
        // tiled's "match against in-memory metadata" fallback so the
        // existing search endpoint still returns sensible results.
        if queries.is_empty() {
            return self.keys();
        }
        self.load_children()
            .iter()
            .filter(|(_, (adapter, access_blob))| {
                queries
                    .iter()
                    .all(|q| matches_query(adapter, access_blob, q))
            })
            .map(|(k, _)| k.clone())
            .collect()
    }
}

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
        _ => true, // Conservative — let unknown queries fall through.
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

    // include_untagged: rows with absent or empty tags are public.
    if f.include_untagged && node_tags.is_empty() {
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
