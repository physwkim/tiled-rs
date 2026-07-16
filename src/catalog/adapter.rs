//! `CatalogAdapter` — exposes the SQL catalog as a [`ContainerAdapter`].
//!
//! Each instance represents one container level: the root has `parent_id =
//! None`; descendants point to their owning row. The async
//! [`ContainerAdapter`] trait lets every method talk to SQL directly:
//! `get` resolves **one** child by key (`fetch_child`) instead of
//! materialising the whole level, `len` is a `count_children`, and
//! `keys`/`search` page the SQL `list_children` result. There is no child
//! cache — every call reflects the latest committed state, so a read after a
//! write in the same request can never see stale rows.
//!
//! The data-source loader picks an adapter shape based on
//! `Node.structure_family`. Leaf families (Array/Table/Sparse/Awkward) are
//! materialised through the registered file-system adapters in the
//! `tiled-adapters` crate via the injected [`LeafResolver`].

use std::sync::Arc;

use crate::core::adapters::{
    AnyAdapter, BaseAdapter, BoxFuture, ContainerAdapter, SearchEntry, SearchPage,
};
use crate::core::queries::{Query, UnsupportedQuery};
use crate::core::schemas::{NodeStructure, SortDirection};
use crate::core::structures::{ContainerStructure, Spec, StructureFamily};

use crate::catalog::db::Catalog;
use crate::catalog::error::CatalogError;
use crate::catalog::orm::Node;

/// Page size for `keys`/`search`, which must enumerate every matching child.
/// `get`/`len` are single round-trips and ignore this.
const PAGE: i64 = 10_000;

/// A container view rooted at a given node (or the catalog root).
pub struct CatalogAdapter {
    catalog: Catalog,
    node_id: Option<i64>,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    /// Resolver that turns a leaf `Node` into a concrete adapter (Array,
    /// Table, ...). Injected by the server (file-system adapters).
    leaf_resolver: Arc<dyn LeafResolver>,
}

/// Strategy that produces a leaf adapter from a node row + its data
/// sources. Different deployments inject different resolvers (file-system,
/// blob, database).
///
/// Async: a resolver does fallible IO (reading the data file's header to
/// build the adapter). It is awaited inside [`ContainerAdapter::get`] on the
/// executor, so a blocking implementation must offload its own file IO to
/// `spawn_blocking` rather than block the runtime.
pub trait LeafResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        catalog: &'a Catalog,
        node: &'a Node,
    ) -> BoxFuture<'a, std::result::Result<AnyAdapter, CatalogError>>;
}

impl CatalogAdapter {
    /// Build the root adapter — the public constructor used by the server.
    pub fn root(catalog: Catalog, leaf_resolver: Arc<dyn LeafResolver>) -> Self {
        Self {
            catalog,
            node_id: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
            specs: vec![Spec::with_version("CatalogOfBlueskyRuns", "1")],
            leaf_resolver,
        }
    }

    fn child_container(&self, node: &Node) -> Self {
        Self {
            catalog: self.catalog.clone(),
            node_id: Some(node.id),
            metadata: node.metadata.clone(),
            specs: parse_specs(&node.specs),
            leaf_resolver: self.leaf_resolver.clone(),
        }
    }

    /// Turn a child `Node` into its adapter: a nested [`CatalogAdapter`] for
    /// containers, or the resolver's leaf adapter otherwise.
    async fn node_to_adapter(&self, node: &Node) -> Result<AnyAdapter, CatalogError> {
        if node.structure_family == "container" {
            Ok(AnyAdapter::Container(Arc::new(self.child_container(node))))
        } else {
            self.leaf_resolver.resolve(&self.catalog, node).await
        }
    }

    /// Direct read access to the underlying [`Catalog`] (write API).
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
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
    fn structure(&self) -> BoxFuture<'_, crate::core::error::Result<ContainerStructure>> {
        Box::pin(async move {
            Ok(ContainerStructure {
                keys: self.keys().await?,
            })
        })
    }

    /// Resolve a single child by key — one `fetch_child` round-trip, no
    /// materialisation of the rest of the level. `Ok(None)` means the key is
    /// absent; an `Err` means the lookup (or leaf resolution) genuinely
    /// failed and must not be collapsed into "absent".
    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, crate::core::error::Result<Option<AnyAdapter>>> {
        Box::pin(async move {
            let Some(node) = self.catalog.fetch_child(self.node_id, key).await? else {
                return Ok(None);
            };
            Ok(Some(self.node_to_adapter(&node).await?))
        })
    }

    fn keys(&self) -> BoxFuture<'_, crate::core::error::Result<Vec<String>>> {
        Box::pin(async move {
            let mut keys = Vec::new();
            let mut offset: i64 = 0;
            loop {
                let nodes = self
                    .catalog
                    .list_children(self.node_id, offset, PAGE)
                    .await?;
                let got = nodes.len();
                keys.extend(nodes.into_iter().map(|n| n.key));
                if got < PAGE as usize {
                    break;
                }
                offset += got as i64;
            }
            Ok(keys)
        })
    }

    fn len(&self) -> BoxFuture<'_, crate::core::error::Result<usize>> {
        Box::pin(async move { Ok(self.catalog.count_children(self.node_id).await? as usize) })
    }

    fn search<'a>(
        &'a self,
        queries: &'a [Query],
    ) -> BoxFuture<'a, crate::core::error::Result<Vec<String>>> {
        Box::pin(async move {
            // Validate every query type up front (before the empty-container
            // shortcut and before per-node filtering) so an unsupported variant
            // yields HTTP 400 regardless of node count or query order — matching
            // Python tiled, which raises UnsupportedQueryType at query dispatch.
            // `?` converts UnsupportedQuery → TiledError::UnsupportedQuery.
            for q in queries {
                ensure_supported(q)?;
            }
            if queries.is_empty() {
                return self.keys().await;
            }
            // Screen each child against the supported predicates using the row
            // fields directly — metadata/structure_family/access_blob all live
            // on `Node`, so there's no need to resolve the (possibly file-backed)
            // leaf adapter just to filter. This also means a node whose data
            // file is missing stays visible to a metadata/access search.
            let mut matched = Vec::new();
            let mut offset: i64 = 0;
            loop {
                let nodes = self
                    .catalog
                    .list_children(self.node_id, offset, PAGE)
                    .await?;
                let got = nodes.len();
                for node in &nodes {
                    if queries.iter().all(|q| {
                        matches_query(&node.metadata, &node.structure_family, &node.access_blob, q)
                    }) {
                        matched.push(node.key.clone());
                    }
                }
                if got < PAGE as usize {
                    break;
                }
                offset += got as i64;
            }
            Ok(matched)
        })
    }

    /// Push the whole listing down to SQL: `search_children` evaluates every
    /// query variant the database supports, applies `ORDER BY`, and returns
    /// just the `[offset, offset+limit)` page plus the total match count. Each
    /// row becomes a [`SearchEntry`] built from its `Node` columns — a
    /// container's structure is a child count (`count_children`), a leaf's is
    /// its first `data_source.structure` — so no (possibly file-backed) leaf
    /// adapter is resolved merely to list it. `access_blob` is carried through
    /// from the row. This is the authoritative search evaluator; it supports
    /// strictly more variants than the in-memory `search` screening above.
    fn search_page<'a>(
        &'a self,
        queries: &'a [Query],
        sorting: &'a [(String, SortDirection)],
        cursor: Option<i64>,
        offset: usize,
        limit: usize,
        include_data_sources: bool,
    ) -> BoxFuture<'a, crate::core::error::Result<SearchPage>> {
        Box::pin(async move {
            // Default sort = no user sort key (the ORDER BY collapses to the
            // monotonic `id`), the only order a keyset cursor can track.
            let is_default = sorting.iter().all(|(k, _)| k.is_empty());
            let (rows, total, next_cursor) = match cursor {
                Some(c) => {
                    // A cursor only addresses the default sort order; reject the
                    // combination (Python raises ValueError → 400).
                    if !is_default {
                        return Err(crate::core::error::TiledError::UnsupportedQuery(
                            "page[cursor] is not supported with a non-default sort order".into(),
                        ));
                    }
                    self.catalog
                        .search_children_cursor(
                            self.node_id,
                            queries,
                            sorting,
                            Some(c),
                            limit as i64,
                        )
                        .await?
                }
                None => {
                    let (rows, total) = self
                        .catalog
                        .search_children(
                            self.node_id,
                            queries,
                            sorting,
                            offset as i64,
                            limit as i64,
                        )
                        .await?;
                    // Default sort: hand back a keyset cursor for the next page
                    // so the server emits a `page[cursor]` next link (N3). The
                    // last row's id is the cursor because the default ORDER BY
                    // ends in the monotonic `id`; `None` when no rows remain or
                    // the sort is non-default (offset pagination only).
                    let next_cursor = if is_default && (offset as i64 + limit as i64) < total {
                        rows.last().map(|n| n.id)
                    } else {
                        None
                    };
                    (rows, total, next_cursor)
                }
            };
            // When include_data_sources is set, batch-load every page node's
            // data sources (+ assets) in two queries — one IN-clause over the
            // page's node ids per table — instead of a query per row. Left
            // `None` when the flag is off so the default listing pays nothing.
            let mut ds_by_node = if include_data_sources {
                let node_ids: Vec<i64> = rows.iter().map(|n| n.id).collect();
                Some(self.catalog.list_data_sources_for_nodes(&node_ids).await?)
            } else {
                None
            };

            let mut entries = Vec::with_capacity(rows.len());
            for node in rows {
                // A row with an unrecognised family defaults to Container
                // (a never-stored case; parity with the prior search path).
                let family = node
                    .structure_family
                    .parse::<StructureFamily>()
                    .unwrap_or(StructureFamily::Container);
                // This node's data sources from the batch fetch. `Some(vec![])`
                // for a node with none (a container, or a leaf with no source) —
                // matching Python's empty `entry.data_sources`; `None` when the
                // flag was off.
                let node_ds = ds_by_node
                    .as_mut()
                    .map(|map| map.remove(&node.id).unwrap_or_default());
                let structure = if matches!(family, StructureFamily::Container) {
                    let count = self.catalog.count_children(Some(node.id)).await?;
                    Some(
                        serde_json::to_value(NodeStructure {
                            contents: None,
                            count: count as usize,
                        })
                        .expect("NodeStructure is always serializable"),
                    )
                } else if let Some(list) = node_ds.as_ref() {
                    // Reuse the batch result: a leaf's structure is its first
                    // data source's, same as the per-row path below.
                    list.first().map(|(ds, _)| ds.structure.clone())
                } else {
                    let ds_rows = self.catalog.list_data_sources(node.id).await?;
                    ds_rows.first().map(|ds| ds.structure.clone())
                };
                // Convert the ORM rows to the wire type. `None` (flag off) stays
                // omitted; `Some` (flag on) carries the list, possibly empty.
                let data_sources = node_ds.map(|list| {
                    list.into_iter()
                        .map(|(ds, assets)| {
                            crate::catalog::data_source::to_core_data_source(ds, assets)
                        })
                        .collect()
                });
                entries.push(SearchEntry {
                    key: node.key,
                    structure_family: family,
                    // Specs are stored as `[{name, version}]`; mirror the
                    // metadata endpoint's `from_value` decode (not the lenient
                    // `parse_specs`) so a search row matches its metadata row.
                    specs: serde_json::from_value(node.specs).unwrap_or_default(),
                    metadata: node.metadata,
                    structure,
                    access_blob: Some(node.access_blob),
                    data_sources,
                });
            }
            Ok(SearchPage {
                entries,
                total: total as usize,
                next_cursor,
            })
        })
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

/// Resolve a (possibly dotted) metadata key to its JSON value, walking nested
/// objects: `"a.b"` descends `meta["a"]["b"]`. Returns `None` when any segment
/// is absent (or an intermediate isn't an object); a present-but-null leaf
/// returns `Some(Value::Null)`. Mirrors the SQL path's
/// `json_extract(metadata, '$.a.b')` / `metadata -> '$.a.b'`
/// (search.rs `Dialect::json_text`) so this in-memory screen and the
/// authoritative SQL evaluator agree on nested keys (catalog M3).
fn nested_get<'a>(meta: &'a serde_json::Value, key: &str) -> Option<&'a serde_json::Value> {
    let mut cur = meta;
    for seg in key.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// Evaluate one query filter against a child node's row fields.
///
/// This path is called only from `CatalogAdapter::search` (container_full
/// export and access-control screening). The SQL path
/// (`Catalog::search_children`) is the authoritative evaluator for the main
/// search endpoint; this screens directly off the `Node` row
/// (`metadata`/`structure_family`/`access_blob`) so it never has to resolve a
/// file-backed leaf adapter just to filter.
fn matches_query(
    meta: &serde_json::Value,
    structure_family: &str,
    access_blob: &serde_json::Value,
    query: &Query,
) -> bool {
    use Query::*;
    match query {
        FullText(q) => meta.to_string().contains(&q.text),
        Eq(eq) => nested_get(meta, &eq.key).is_some_and(|v| v == &eq.value),
        NotEq(neq) => nested_get(meta, &neq.key).is_none_or(|v| v != &neq.value),
        KeyPresent(kp) => nested_get(meta, &kp.key).is_some() == kp.exists,
        StructureFamily(sf) => structure_family == sf.value.to_string(),
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
    f: &crate::core::queries::AccessBlobFilter,
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
    fn resolve<'a>(
        &'a self,
        _catalog: &'a Catalog,
        node: &'a Node,
    ) -> BoxFuture<'a, std::result::Result<AnyAdapter, CatalogError>> {
        Box::pin(async move {
            Err(CatalogError::Validation(format!(
                "no leaf resolver registered for structure_family={} (node {})",
                node.structure_family, node.key
            )))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::core::queries::{
        AccessBlobFilter, Eq as EqQ, In as InQ, KeyLookup, KeyPresent as KeyPresentQ, KeysFilter,
        NotEq as NotEqQ, NotIn as NotInQ, Query,
    };
    use serde_json::json;

    use super::{ensure_supported, matches_query};

    // `search` screens off the `Node` row directly, so `matches_query` now
    // takes the raw (metadata, structure_family, access_blob) fields. The
    // family is irrelevant to every query here except `StructureFamily`
    // (which `ensure_supported` allows but no test below exercises), so the
    // helpers pass a fixed "container".
    const FAMILY: &str = "container";

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
        let meta = json!({});
        let blob = json!({});
        let q = Query::Lookup(KeyLookup {
            key: "anything".into(),
        });
        // Lookup stays supported here (the SQL path resolves keys), unlike the
        // map/mongo adapters where it has no pre-filter and errors.
        assert!(ensure_supported(&q).is_ok(), "Lookup must stay supported");
        assert!(
            matches_query(&meta, FAMILY, &blob, &q),
            "Lookup must pass through"
        );
    }

    #[test]
    fn keys_filter_passes_through() {
        let meta = json!({});
        let blob = json!({});
        let q = Query::KeysFilter(KeysFilter {
            keys: vec!["k".into()],
        });
        assert!(
            ensure_supported(&q).is_ok(),
            "KeysFilter must stay supported"
        );
        assert!(
            matches_query(&meta, FAMILY, &blob, &q),
            "KeysFilter must pass through"
        );
    }

    // AccessBlobFilter must still work (the reachable use-case).

    #[test]
    fn access_blob_filter_matches_tagged_node() {
        let meta = json!({});
        let blob = json!({"tags": ["team_a"]});
        let q = Query::AccessBlobFilter(AccessBlobFilter {
            tags: vec!["team_a".into()],
            ..Default::default()
        });
        assert!(matches_query(&meta, FAMILY, &blob, &q));
    }

    #[test]
    fn access_blob_filter_denies_unmatched_node() {
        let meta = json!({});
        let blob = json!({"tags": ["team_b"]});
        let q = Query::AccessBlobFilter(AccessBlobFilter {
            tags: vec!["team_a".into()],
            ..Default::default()
        });
        assert!(!matches_query(&meta, FAMILY, &blob, &q));
    }

    /// Regression (fail-open leak): include_untagged must NOT match a
    /// user-owned blob `{"user": id}` (which has no `tags` key). Such a node
    /// is reachable only by its owner via the user-ownership arm.
    #[test]
    fn access_blob_filter_untagged_excludes_user_owned() {
        let meta = json!({});
        let blob = json!({"user": "bob-uuid"});
        // Anonymous-style filter: no user_id, no tags, only include_untagged.
        let anon = Query::AccessBlobFilter(AccessBlobFilter {
            include_untagged: true,
            ..Default::default()
        });
        assert!(
            !matches_query(&meta, FAMILY, &blob, &anon),
            "user-owned node must not be visible via the untagged-public arm"
        );
        // Cross-user filter: alice's grant must not surface bob's owned node.
        let alice = Query::AccessBlobFilter(AccessBlobFilter {
            user_id: Some("alice-uuid".into()),
            include_untagged: true,
            ..Default::default()
        });
        assert!(
            !matches_query(&meta, FAMILY, &blob, &alice),
            "another user's owned node must not be visible to alice"
        );
        // The owner still sees their own node via the user arm.
        let owner = Query::AccessBlobFilter(AccessBlobFilter {
            user_id: Some("bob-uuid".into()),
            include_untagged: true,
            ..Default::default()
        });
        assert!(
            matches_query(&meta, FAMILY, &blob, &owner),
            "owner must still see their own node"
        );
    }

    // Supported metadata variants (Eq, NotEq) must continue to work.

    #[test]
    fn eq_matches_correct_value() {
        let meta = json!({"x": 1});
        let blob = json!({});
        assert!(matches_query(
            &meta,
            FAMILY,
            &blob,
            &Query::Eq(EqQ {
                key: "x".into(),
                value: json!(1)
            })
        ));
        assert!(!matches_query(
            &meta,
            FAMILY,
            &blob,
            &Query::Eq(EqQ {
                key: "x".into(),
                value: json!(2)
            })
        ));
    }

    // catalog M3: a dotted key addresses a nested path, matching the SQL path's
    // `json_extract(metadata, '$.a.b')` rather than a flat top-level key named
    // literally "a.b".

    #[test]
    fn eq_dotted_key_addresses_nested_value() {
        let meta = json!({"a": {"b": 7}});
        let blob = json!({});
        assert!(
            matches_query(
                &meta,
                FAMILY,
                &blob,
                &Query::Eq(EqQ {
                    key: "a.b".into(),
                    value: json!(7)
                })
            ),
            "dotted Eq must descend into the nested object"
        );
        assert!(
            !matches_query(
                &meta,
                FAMILY,
                &blob,
                &Query::Eq(EqQ {
                    key: "a.b".into(),
                    value: json!(8)
                })
            ),
            "dotted Eq must compare the nested value, not match blindly"
        );
        // A flat top-level key literally named "a.b" does not exist here, so the
        // old `meta.get("a.b")` would have failed to match the nested 7.
        assert!(
            !matches_query(
                &meta,
                FAMILY,
                &blob,
                &Query::Eq(EqQ {
                    key: "missing.path".into(),
                    value: json!(7)
                })
            ),
            "absent nested path must not match"
        );
    }

    #[test]
    fn key_present_dotted_key_checks_nested_path() {
        let meta = json!({"a": {"b": 1}});
        let blob = json!({});
        assert!(matches_query(
            &meta,
            FAMILY,
            &blob,
            &Query::KeyPresent(KeyPresentQ {
                key: "a.b".into(),
                exists: true
            })
        ));
        assert!(matches_query(
            &meta,
            FAMILY,
            &blob,
            &Query::KeyPresent(KeyPresentQ {
                key: "a.c".into(),
                exists: false
            })
        ));
    }

    #[test]
    fn not_eq_dotted_key_includes_missing_path_like_sql() {
        // The catalog SQL NotEq is `(lhs IS NULL OR lhs != value)`, so a node
        // missing the (nested) key is "not equal" and stays in the result. The
        // in-memory screen mirrors that.
        let blob = json!({});
        let present_equal = json!({"a": {"b": 5}});
        assert!(
            !matches_query(
                &present_equal,
                FAMILY,
                &blob,
                &Query::NotEq(NotEqQ {
                    key: "a.b".into(),
                    value: json!(5)
                })
            ),
            "present-and-equal nested value must be excluded by NotEq"
        );
        let present_different = json!({"a": {"b": 6}});
        assert!(matches_query(
            &present_different,
            FAMILY,
            &blob,
            &Query::NotEq(NotEqQ {
                key: "a.b".into(),
                value: json!(5)
            })
        ));
        let missing = json!({"a": {"z": 0}});
        assert!(
            matches_query(
                &missing,
                FAMILY,
                &blob,
                &Query::NotEq(NotEqQ {
                    key: "a.b".into(),
                    value: json!(5)
                })
            ),
            "missing nested path must be included by NotEq (SQL parity)"
        );
    }
}
