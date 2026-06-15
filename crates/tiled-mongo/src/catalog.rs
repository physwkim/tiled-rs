//! Top-level MongoDB catalog adapter.
//!
//! Corresponds to `databroker.mongo_normalized.MongoAdapter`.
//! Lists BlueskyRuns from the `run_start` collection.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use indexmap::IndexMap;
use mongodb::bson::{Document, doc};
use mongodb::sync::Database;

use tiled_core::adapters::{AnyAdapter, BaseAdapter, ContainerAdapter};
use tiled_core::queries::{Query, UnsupportedQuery};
use tiled_core::structures::{ContainerStructure, Spec, StructureFamily};

use crate::run::BlueskyRunAdapter;

/// Top-level catalog: lists all BlueskyRuns in a MongoDB database.
pub struct MongoCatalog {
    db: Database,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    /// Cached mapping of uid → BlueskyRunAdapter (populated on first access).
    runs: OnceLock<IndexMap<String, AnyAdapter>>,
    /// Cached structure (keys list) — kept in stable storage so `structure()`
    /// can return `&ContainerStructure` without `Box::leak`.
    structure_cache: OnceLock<ContainerStructure>,
}

impl MongoCatalog {
    /// Create from a connected MongoDB database.
    pub fn new(db: Database, metadata: serde_json::Value) -> Self {
        Self {
            db,
            metadata,
            specs: vec![Spec::with_version("CatalogOfBlueskyRuns", "1")],
            runs: OnceLock::new(),
            structure_cache: OnceLock::new(),
        }
    }

    /// Connect to MongoDB and create a catalog.
    pub fn from_uri(uri: &str) -> Result<Self, mongodb::error::Error> {
        let client = mongodb::sync::Client::with_uri_str(uri)?;
        // Extract database name from URI: `mongodb://host[:port][/db][?opts]`.
        // The last `/`-separated segment may be empty (no db given), include
        // a `?`-prefixed options block, or simply be the db name.
        let after_scheme = uri.split_once("://").map(|(_, r)| r).unwrap_or(uri);
        let path = after_scheme.split_once('/').map(|(_, p)| p).unwrap_or("");
        let path_no_opts = path.split('?').next().unwrap_or("").trim_matches('/');
        let db_name = if path_no_opts.is_empty() {
            "databroker"
        } else {
            path_no_opts
        };
        let db = client.database(db_name);
        Ok(Self::new(db, serde_json::json!({})))
    }

    fn load_runs(&self) -> &IndexMap<String, AnyAdapter> {
        self.runs.get_or_init(|| {
            let mut mapping = IndexMap::new();
            let collection = self.db.collection::<Document>("run_start");

            // Find all run_start docs, sorted by time descending (newest first).
            let opts = mongodb::options::FindOptions::builder()
                .sort(doc! { "time": -1 })
                .build();

            if let Ok(cursor) = collection.find(doc! {}).with_options(opts).run() {
                // Collect every run_start first (the in-memory catalog design
                // materialises all runs anyway), preserving the cursor's
                // time-descending order.
                let starts: Vec<(String, Document)> = cursor
                    .flatten()
                    .filter_map(|start_doc| {
                        let uid = start_doc.get_str("uid").unwrap_or_default().to_string();
                        (!uid.is_empty()).then_some((uid, start_doc))
                    })
                    .collect();

                // Batch the run_stop lookup into ONE find({run_start: {$in:
                // [...]}}) keyed by run_start, instead of an N+1 find_one per
                // run. A run with no stop doc is left as `None`.
                let uids: Vec<String> = starts.iter().map(|(u, _)| u.clone()).collect();
                let mut stops: HashMap<String, Document> = HashMap::new();
                if !uids.is_empty()
                    && let Ok(stop_cursor) = self
                        .db
                        .collection::<Document>("run_stop")
                        .find(doc! { "run_start": { "$in": uids } })
                        .run()
                {
                    for stop_doc in stop_cursor.flatten() {
                        if let Ok(rs) = stop_doc.get_str("run_start") {
                            stops.insert(rs.to_string(), stop_doc);
                        }
                    }
                }

                for (uid, start_doc, stop_doc) in pair_starts_with_stops(starts, stops) {
                    let run = BlueskyRunAdapter::new(self.db.clone(), start_doc, stop_doc);
                    mapping.insert(uid, AnyAdapter::Container(Arc::new(run)));
                }
            }
            mapping
        })
    }
}

/// Pair each run_start (in cursor order) with its run_stop from the batched
/// `$in` lookup. A run whose uid has no stop doc gets `None`; a stop doc whose
/// `run_start` matches no loaded run is ignored. Pure (no I/O) so the
/// N+1→batch pairing — the part that can scramble order or mismatch a run with
/// the wrong stop — is unit-testable without a live MongoDB.
fn pair_starts_with_stops(
    starts: Vec<(String, Document)>,
    mut stops: HashMap<String, Document>,
) -> Vec<(String, Document, Option<Document>)> {
    starts
        .into_iter()
        .map(|(uid, start_doc)| {
            let stop_doc = stops.remove(&uid);
            (uid, start_doc, stop_doc)
        })
        .collect()
}

impl BaseAdapter for MongoCatalog {
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

impl ContainerAdapter for MongoCatalog {
    fn structure(&self) -> &ContainerStructure {
        self.structure_cache.get_or_init(|| ContainerStructure {
            keys: self.load_runs().keys().cloned().collect(),
        })
    }

    fn get(&self, key: &str) -> Option<&AnyAdapter> {
        self.load_runs().get(key)
    }

    fn keys(&self) -> Vec<String> {
        self.load_runs().keys().cloned().collect()
    }

    fn len(&self) -> usize {
        self.load_runs().len()
    }

    fn search(&self, queries: &[Query]) -> Result<Vec<String>, UnsupportedQuery> {
        // Validate every query type up front (before the empty-container
        // shortcut and per-run filtering) so an unsupported variant yields
        // HTTP 400 regardless of run count or query order — parity with
        // Python tiled, which raises UnsupportedQueryType at query dispatch.
        for q in queries {
            ensure_supported(q)?;
        }
        if queries.is_empty() {
            return Ok(self.keys());
        }
        Ok(self
            .load_runs()
            .iter()
            .filter(|(_, adapter)| queries.iter().all(|q| matches_run_query(adapter, q)))
            .map(|(k, _)| k.clone())
            .collect())
    }
}

/// Single source of truth for which query variants this in-memory run-metadata
/// path can evaluate. `search` gates every query through this before calling
/// `matches_run_query`. Unlike `tiled-catalog`, tiled-mongo has no SQL/Mongo
/// pre-filter pass that resolves Lookup/KeysFilter or AccessBlobFilter before
/// this function runs, so those are unsupported here too. Unsupported variants
/// become `UnsupportedQuery` → HTTP 400 (parity: Python `UnsupportedQueryType`).
fn ensure_supported(query: &Query) -> Result<(), UnsupportedQuery> {
    use Query::*;
    match query {
        FullText(_) | StructureFamily(_) | Eq(_) | NotEq(_) | KeyPresent(_) | Contains(_)
        | In(_) | NotIn(_) | Comparison(_) | Specs(_) => Ok(()),
        // Like, Regex, Lookup, KeysFilter, AccessBlobFilter: no in-memory or
        // pre-filter path evaluates these for a BlueskyRun. Surface as 400
        // rather than silently returning a filtered subset.
        other => Err(UnsupportedQuery(other.type_name().to_string())),
    }
}

/// Match a query against a BlueskyRun's metadata. Searches keys inside
/// `metadata.start` first (the natural place for run-level fields like
/// `plan_name`, `sample`, …), falling back to top-level metadata.
fn matches_run_query(adapter: &AnyAdapter, query: &Query) -> bool {
    let meta = adapter.metadata();
    let lookup = |key: &str| -> Option<&serde_json::Value> {
        meta.get("start")
            .and_then(|s| s.get(key))
            .or_else(|| meta.get(key))
    };
    match query {
        Query::FullText(ft) => meta.to_string().contains(&ft.text),
        Query::StructureFamily(sf) => adapter.structure_family() == sf.value,
        Query::Eq(eq) => lookup(&eq.key).is_some_and(|v| v == &eq.value),
        Query::NotEq(neq) => lookup(&neq.key).is_none_or(|v| v != &neq.value),
        Query::KeyPresent(kp) => lookup(&kp.key).is_some() == kp.exists,
        Query::Contains(c) => lookup(&c.key)
            .and_then(|v| v.as_array())
            .is_some_and(|arr| arr.contains(&c.value)),
        Query::In(q) => lookup(&q.key).is_some_and(|v| q.value.iter().any(|x| x == v)),
        Query::NotIn(q) => lookup(&q.key).is_none_or(|v| !q.value.iter().any(|x| x == v)),
        Query::Comparison(c) => lookup(&c.key).is_some_and(|v| {
            use std::cmp::Ordering;
            use tiled_core::queries::Operator;
            let cmp = match (v.as_f64(), c.value.as_f64()) {
                (Some(a), Some(b)) => a.partial_cmp(&b),
                _ => match (v.as_str(), c.value.as_str()) {
                    (Some(a), Some(b)) => Some(a.cmp(b)),
                    _ => None,
                },
            };
            matches!(
                (c.operator, cmp),
                (Operator::Lt, Some(Ordering::Less))
                    | (Operator::Gt, Some(Ordering::Greater))
                    | (Operator::Le, Some(Ordering::Less | Ordering::Equal))
                    | (Operator::Ge, Some(Ordering::Greater | Ordering::Equal))
            )
        }),
        Query::Specs(s) => {
            let names: std::collections::HashSet<_> =
                adapter.specs().iter().map(|sp| sp.name.as_str()).collect();
            s.include.iter().all(|n| names.contains(n.as_str()))
                && !s.exclude.iter().any(|n| names.contains(n.as_str()))
        }
        // Unimplemented variants (Like, Regex, Lookup, KeysFilter,
        // AccessBlobFilter) cannot be evaluated in-memory here — there is no
        // separate MongoDB or SQL path that pre-filters them before this
        // function is called (unlike tiled-catalog where Lookup/KeysFilter are
        // resolved by SQL before the in-memory pass). `search` rejects them up
        // front via `ensure_supported` (→ HTTP 400, parity with
        // UnsupportedQueryType), so this arm is unreachable through the normal
        // path; keep it as a fail-closed default for any direct caller.
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use tiled_core::adapters::{AnyAdapter, BaseAdapter, ContainerAdapter};
    use tiled_core::queries::{AccessBlobFilter, KeyLookup, KeysFilter, Like, Query, Regex};
    use tiled_core::structures::{ContainerStructure, Spec, StructureFamily};

    use tiled_core::queries::NotIn as NotInQ;

    use super::{ensure_supported, matches_run_query, pair_starts_with_stops};

    struct StubRun {
        metadata: serde_json::Value,
    }

    impl BaseAdapter for StubRun {
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

    impl ContainerAdapter for StubRun {
        fn structure(&self) -> &ContainerStructure {
            static S: ContainerStructure = ContainerStructure { keys: vec![] };
            &S
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

    fn run(meta: serde_json::Value) -> AnyAdapter {
        AnyAdapter::Container(Arc::new(StubRun { metadata: meta }))
    }

    // Unsupported variants must surface as UnsupportedQuery (→ HTTP 400),
    // not silently filter to a subset. tiled-mongo has no SQL/Mongo pre-filter
    // pass, so Like/Regex/Lookup/KeysFilter/AccessBlobFilter are all
    // unsupported here.

    #[test]
    fn like_returns_error() {
        let q = Query::Like(Like {
            key: "plan_name".into(),
            pattern: "scan".into(),
        });
        assert_eq!(
            ensure_supported(&q).unwrap_err().0,
            "Like",
            "Like is unimplemented and must error, not silently pass"
        );
    }

    #[test]
    fn regex_returns_error() {
        let q = Query::Regex(Regex {
            key: "plan_name".into(),
            pattern: "scan".into(),
            case_sensitive: true,
        });
        assert_eq!(ensure_supported(&q).unwrap_err().0, "Regex");
    }

    #[test]
    fn lookup_returns_error() {
        // Lookup is key-based. In tiled-mongo there is no SQL pre-filter path
        // that resolves keys before search — unlike tiled-catalog where Lookup
        // passes through because SQL already handled it.
        let q = Query::Lookup(KeyLookup {
            key: "any_uid".into(),
        });
        assert_eq!(ensure_supported(&q).unwrap_err().0, "KeyLookup");
    }

    #[test]
    fn keys_filter_returns_error() {
        let q = Query::KeysFilter(KeysFilter {
            keys: vec!["k".into()],
        });
        assert_eq!(ensure_supported(&q).unwrap_err().0, "KeysFilter");
    }

    #[test]
    fn access_blob_filter_returns_error() {
        // Unlike tiled-catalog/tiled-adapters, a BlueskyRun carries no
        // access_blob, so AccessBlobFilter cannot be evaluated here and must
        // error rather than excluding every run.
        let q = Query::AccessBlobFilter(AccessBlobFilter {
            tags: vec!["team_a".into()],
            ..Default::default()
        });
        assert_eq!(ensure_supported(&q).unwrap_err().0, "AccessBlobFilter");
    }

    // Implemented variants must continue to work correctly.

    // NotIn semantics: a run whose key is absent must be INCLUDED (mirrors
    // MongoDB $nin and SQL "IS NULL OR NOT IN (...)").

    #[test]
    fn notin_includes_run_missing_key() {
        // run has no "detector" key at all — NotIn must pass it through.
        let a = run(json!({"start": {"plan_name": "scan"}}));
        let q = Query::NotIn(NotInQ {
            key: "detector".into(),
            value: vec![json!("area"), json!("strip")],
        });
        assert!(
            matches_run_query(&a, &q),
            "NotIn must include runs that lack the queried key (missing ≠ excluded)"
        );
    }

    #[test]
    fn notin_excludes_run_with_matching_value() {
        let a = run(json!({"start": {"detector": "area"}}));
        let q = Query::NotIn(NotInQ {
            key: "detector".into(),
            value: vec![json!("area"), json!("strip")],
        });
        assert!(
            !matches_run_query(&a, &q),
            "NotIn must exclude runs whose key value is in the list"
        );
    }

    #[test]
    fn notin_includes_run_with_non_matching_value() {
        let a = run(json!({"start": {"detector": "pixel"}}));
        let q = Query::NotIn(NotInQ {
            key: "detector".into(),
            value: vec![json!("area"), json!("strip")],
        });
        assert!(
            matches_run_query(&a, &q),
            "NotIn must include runs whose key value is not in the list"
        );
    }

    #[test]
    fn eq_on_start_field_matches() {
        let a = run(json!({"start": {"plan_name": "scan"}}));
        let q = Query::Eq(tiled_core::queries::Eq {
            key: "plan_name".into(),
            value: json!("scan"),
        });
        assert!(matches_run_query(&a, &q));
    }

    #[test]
    fn eq_on_start_field_does_not_match_wrong_value() {
        let a = run(json!({"start": {"plan_name": "scan"}}));
        let q = Query::Eq(tiled_core::queries::Eq {
            key: "plan_name".into(),
            value: json!("count"),
        });
        assert!(!matches_run_query(&a, &q));
    }

    // H7: the batched run_stop lookup replaces an N+1 find_one per run. The
    // pairing of each run_start with the right run_stop (and the order/missing
    // handling) is the part that can regress; cover it without a live MongoDB.

    use mongodb::bson::{Document, doc};
    use std::collections::HashMap;

    fn start(uid: &str) -> (String, Document) {
        (uid.to_string(), doc! { "uid": uid })
    }

    #[test]
    fn pairs_each_run_with_its_own_stop_preserving_order() {
        // Cursor order is time-descending; pairing must not reorder.
        let starts = vec![start("c"), start("b"), start("a")];
        let mut stops = HashMap::new();
        stops.insert(
            "a".to_string(),
            doc! { "run_start": "a", "exit_status": "success" },
        );
        stops.insert(
            "b".to_string(),
            doc! { "run_start": "b", "exit_status": "abort" },
        );
        stops.insert(
            "c".to_string(),
            doc! { "run_start": "c", "exit_status": "fail" },
        );

        let paired = pair_starts_with_stops(starts, stops);

        let uids: Vec<&str> = paired.iter().map(|(u, _, _)| u.as_str()).collect();
        assert_eq!(
            uids,
            vec!["c", "b", "a"],
            "order must follow the cursor, not the HashMap"
        );
        // Each run carries its OWN stop, not another run's.
        for (uid, _, stop) in &paired {
            let got = stop.as_ref().unwrap().get_str("run_start").unwrap();
            assert_eq!(got, uid, "run {uid} was paired with the wrong stop ({got})");
        }
    }

    #[test]
    fn run_without_stop_gets_none() {
        let starts = vec![start("a"), start("b")];
        let mut stops = HashMap::new();
        stops.insert("a".to_string(), doc! { "run_start": "a" });
        // "b" has no stop doc (run still in progress).

        let paired = pair_starts_with_stops(starts, stops);

        assert!(paired[0].2.is_some(), "run a has a stop");
        assert!(
            paired[1].2.is_none(),
            "run b without a stop must be None, not another run's stop"
        );
    }

    #[test]
    fn orphan_stop_does_not_create_a_phantom_run() {
        // A run_stop whose run_start matches no loaded run must be ignored —
        // it must not appear as an extra run.
        let starts = vec![start("a")];
        let mut stops = HashMap::new();
        stops.insert("a".to_string(), doc! { "run_start": "a" });
        stops.insert("orphan".to_string(), doc! { "run_start": "orphan" });

        let paired = pair_starts_with_stops(starts, stops);

        assert_eq!(
            paired.len(),
            1,
            "only loaded runs may appear; orphan stop is dropped"
        );
        assert_eq!(paired[0].0, "a");
    }
}
