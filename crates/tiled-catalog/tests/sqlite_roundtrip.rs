//! SQLite round-trip — open in-memory, migrate, write, read.

use serde_json::json;
use tiled_core::queries::{Eq, In, Like, NotEq, NotIn, Query, SpecsQuery};
use tiled_core::schemas::SortDirection;

use tiled_catalog::data_source::{AssetSpec, DataSourceSpec};
use tiled_catalog::{Catalog, RegisterRequest};

#[tokio::test]
async fn migrate_create_lookup_delete() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("catalog.db");
    let uri = format!("sqlite://{}", path.display());

    let cat = Catalog::connect(&uri).await.unwrap();
    cat.migrate().await.unwrap();
    assert_eq!(
        cat.applied_migrations().await.unwrap(),
        vec![
            "0001_initial".to_string(),
            "0002_webhooks".to_string(),
            "0003_revisions_access_blob".to_string(),
            "0004_metadata_fts".to_string(),
        ],
    );

    // No nodes yet.
    assert_eq!(cat.count_children(None).await.unwrap(), 0);

    // Create a container at the root.
    let container = cat
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: "experiment_a".into(),
                structure_family: "container".into(),
                metadata: json!({"description": "first"}),
                specs: json!([{"name": "BlueskyRun", "version": "1"}]),
                access_blob: json!({}),
            },
        )
        .await
        .unwrap();
    assert!(container.id > 0);
    assert_eq!(container.key, "experiment_a");

    // Lookup by path.
    let found = cat.lookup(&["experiment_a".into()]).await.unwrap().unwrap();
    assert_eq!(found.id, container.id);
    assert_eq!(found.metadata, json!({"description": "first"}));

    // Create a child array node + data source.
    let array = cat
        .create_node(
            Some(container.id),
            vec!["experiment_a".into()],
            RegisterRequest {
                key: "frame".into(),
                structure_family: "array".into(),
                metadata: json!({"detector": "andor"}),
                specs: json!([]),
                access_blob: json!({}),
            },
        )
        .await
        .unwrap();
    let ds = cat
        .create_data_source(
            array.id,
            DataSourceSpec {
                structure_family: "array".into(),
                structure: json!({
                    "shape": [10, 10],
                    "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
                    "chunks": [[10], [10]],
                }),
                mimetype: "application/x-hdf5".into(),
                parameters: json!({}),
                management: "external".into(),
                assets: vec![AssetSpec {
                    data_uri: "file:///tmp/frame.h5".into(),
                    is_directory: false,
                    parameter: "data_uri".into(),
                    num: None,
                }],
            },
        )
        .await
        .unwrap();
    assert_eq!(ds.node_id, array.id);

    let assets = cat.list_assets(ds.id).await.unwrap();
    assert_eq!(assets.len(), 1);
    assert_eq!(assets[0].data_uri, "file:///tmp/frame.h5");

    // Update metadata; should write a revision row.
    let updated = cat
        .update_metadata(
            container.id,
            json!({"description": "second"}),
            json!([{"name": "BlueskyRun", "version": "2"}]),
            /* drop_revision */ false,
        )
        .await
        .unwrap();
    assert_eq!(updated.metadata, json!({"description": "second"}));

    // Children listing.
    let kids = cat.list_children(Some(container.id), 0, 100).await.unwrap();
    assert_eq!(kids.len(), 1);
    assert_eq!(kids[0].key, "frame");

    // Delete cascades. The only descendant data source is `external`, so the
    // default safety gate (external_only=true) permits it.
    cat.delete_node(container.id, true).await.unwrap();
    assert!(
        cat.lookup(&["experiment_a".into()])
            .await
            .unwrap()
            .is_none()
    );
    let assets_after = cat.list_assets(ds.id).await.unwrap();
    assert_eq!(assets_after.len(), 0);
}

/// F-F: `delete_node` with `external_only=true` (the safe default) refuses to
/// delete a subtree that holds an internally-managed data source, mirroring
/// Python `WouldDeleteData` (adapter.py:1037-1055). Passing `external_only=false`
/// forces the cascade.
#[tokio::test]
async fn delete_refuses_internally_managed_subtree() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let cat = Catalog::connect(&uri).await.unwrap();
    cat.migrate().await.unwrap();

    // container/frame, where `frame` carries a *writable* (internally-managed)
    // data source nested one level below the delete target.
    let container = cat
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: "expt".into(),
                structure_family: "container".into(),
                metadata: json!({}),
                specs: json!([]),
                access_blob: json!({}),
            },
        )
        .await
        .unwrap();
    let array = cat
        .create_node(
            Some(container.id),
            vec!["expt".into()],
            RegisterRequest {
                key: "frame".into(),
                structure_family: "array".into(),
                metadata: json!({}),
                specs: json!([]),
                access_blob: json!({}),
            },
        )
        .await
        .unwrap();
    cat.create_data_source(
        array.id,
        DataSourceSpec {
            structure_family: "array".into(),
            structure: json!({
                "shape": [10],
                "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
                "chunks": [[10]],
            }),
            mimetype: "application/x-hdf5".into(),
            parameters: json!({}),
            management: "writable".into(),
            assets: vec![AssetSpec {
                data_uri: "file:///tmp/frame.h5".into(),
                is_directory: false,
                parameter: "data_uri".into(),
                num: None,
            }],
        },
    )
    .await
    .unwrap();

    // Default gate refuses, even when deleting an ancestor of the managed node.
    let err = cat.delete_node(container.id, true).await.unwrap_err();
    assert!(
        matches!(err, tiled_catalog::CatalogError::WouldDeleteData(_)),
        "expected WouldDeleteData, got {err:?}"
    );
    // Nothing was deleted.
    assert!(cat.lookup(&["expt".into()]).await.unwrap().is_some());

    // Forced delete (external_only=false) cascades through.
    cat.delete_node(container.id, false).await.unwrap();
    assert!(cat.lookup(&["expt".into()]).await.unwrap().is_none());
    assert!(
        cat.lookup(&["expt".into(), "frame".into()])
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn metadata_size_limit_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let cat = Catalog::connect(&uri).await.unwrap();
    cat.migrate().await.unwrap();
    let big = "x".repeat(11 * 1024 * 1024);
    let err = cat
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: "huge".into(),
                structure_family: "container".into(),
                metadata: json!({"blob": big}),
                specs: json!([]),
                access_blob: json!({}),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, tiled_catalog::CatalogError::Validation(_)));
}

#[tokio::test]
async fn duplicate_key_at_same_level_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let cat = Catalog::connect(&uri).await.unwrap();
    cat.migrate().await.unwrap();
    let req = || RegisterRequest {
        key: "dup".into(),
        structure_family: "container".into(),
        metadata: json!({}),
        specs: json!([]),
        access_blob: json!({}),
    };
    cat.create_node(None, vec![], req()).await.unwrap();
    let err = cat.create_node(None, vec![], req()).await.unwrap_err();
    assert!(matches!(err, tiled_catalog::CatalogError::Conflict(_)));
}

/// H2: Like returns only the matching subset, not the whole container.
#[tokio::test]
async fn search_like_filters_correct_subset() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let cat = Catalog::connect(&uri).await.unwrap();
    cat.migrate().await.unwrap();

    for (key, material) in [("cu_run", "Cu"), ("ni_run", "Ni"), ("fe_run", "Fe")] {
        cat.create_node(
            None,
            vec![],
            RegisterRequest {
                key: key.into(),
                structure_family: "container".into(),
                metadata: json!({"material": material}),
                specs: json!([]),
                access_blob: json!({}),
            },
        )
        .await
        .unwrap();
    }

    // LIKE "Cu" — exact match (no wildcards in pattern)
    let (nodes, total) = cat
        .search_children(
            None,
            &[Query::Like(Like {
                key: "material".into(),
                pattern: "Cu".into(),
            })],
            &[],
            0,
            100,
        )
        .await
        .unwrap();
    assert_eq!(total, 1, "Like 'Cu' should match exactly 1 node");
    assert_eq!(nodes[0].key, "cu_run");

    // LIKE "N%" — prefix wildcard
    let (nodes2, total2) = cat
        .search_children(
            None,
            &[Query::Like(Like {
                key: "material".into(),
                pattern: "N%".into(),
            })],
            &[],
            0,
            100,
        )
        .await
        .unwrap();
    assert_eq!(total2, 1, "Like 'N%' should match exactly 1 node");
    assert_eq!(nodes2[0].key, "ni_run");
}

/// H2: Specs(include) returns only nodes whose specs column contains every
/// listed spec name. Specs(exclude) excludes them.
#[tokio::test]
async fn search_specs_include_and_exclude() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let cat = Catalog::connect(&uri).await.unwrap();
    cat.migrate().await.unwrap();

    // "xas_run" has BlueskyRun + XAS; "nd_run" has BlueskyRun + NXdata; "bare" has none.
    for (key, specs) in [
        (
            "xas_run",
            json!([{"name": "BlueskyRun", "version": "1"}, {"name": "XAS", "version": "1"}]),
        ),
        (
            "nd_run",
            json!([{"name": "BlueskyRun", "version": "1"}, {"name": "NXdata", "version": "1"}]),
        ),
        ("bare", json!([])),
    ] {
        cat.create_node(
            None,
            vec![],
            RegisterRequest {
                key: key.into(),
                structure_family: "container".into(),
                metadata: json!({}),
                specs,
                access_blob: json!({}),
            },
        )
        .await
        .unwrap();
    }

    // include=["XAS"] → only xas_run
    let (nodes, total) = cat
        .search_children(
            None,
            &[Query::Specs(SpecsQuery {
                include: vec!["XAS".into()],
                exclude: vec![],
            })],
            &[],
            0,
            100,
        )
        .await
        .unwrap();
    assert_eq!(total, 1, "include=[XAS] should match 1 node");
    assert_eq!(nodes[0].key, "xas_run");

    // include=["BlueskyRun"] → both xas_run and nd_run
    let (_nodes2, total2) = cat
        .search_children(
            None,
            &[Query::Specs(SpecsQuery {
                include: vec!["BlueskyRun".into()],
                exclude: vec![],
            })],
            &[],
            0,
            100,
        )
        .await
        .unwrap();
    assert_eq!(total2, 2, "include=[BlueskyRun] should match 2 nodes");

    // exclude=["XAS"] → nd_run and bare (everything without XAS)
    let (nodes3, total3) = cat
        .search_children(
            None,
            &[Query::Specs(SpecsQuery {
                include: vec![],
                exclude: vec!["XAS".into()],
            })],
            &[],
            0,
            100,
        )
        .await
        .unwrap();
    assert_eq!(
        total3, 2,
        "exclude=[XAS] should match 2 nodes (nd_run + bare)"
    );
    let keys: Vec<&str> = nodes3.iter().map(|n| n.key.as_str()).collect();
    assert!(!keys.contains(&"xas_run"), "xas_run must be excluded");
}

/// H1: Eq/NotEq/In/NotIn on NUMERIC metadata fields must match under SQLite.
///
/// SQLite's `json_extract` returns the native storage class (INTEGER for JSON
/// integers). Binding the filter value as TEXT ('5') produces a no-match
/// because SQLite's no-affinity rules never coerce INTEGER to TEXT. This test
/// fails on the old code (all return 0 rows) and passes after the fix.
#[tokio::test]
async fn search_numeric_eq_in_notin_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let cat = Catalog::connect(&uri).await.unwrap();
    cat.migrate().await.unwrap();

    // Three nodes with integer metadata fields.
    for (key, scan_id, count) in [("run_1", 1i64, 10i64), ("run_2", 2, 20), ("run_3", 3, 10)] {
        cat.create_node(
            None,
            vec![],
            RegisterRequest {
                key: key.into(),
                structure_family: "container".into(),
                metadata: json!({"scan_id": scan_id, "count": count}),
                specs: json!([]),
                access_blob: json!({}),
            },
        )
        .await
        .unwrap();
    }

    // Eq: scan_id == 2 → only run_2
    let (nodes, total) = cat
        .search_children(
            None,
            &[Query::Eq(Eq {
                key: "scan_id".into(),
                value: json!(2),
            })],
            &[],
            0,
            100,
        )
        .await
        .unwrap();
    assert_eq!(total, 1, "Eq(scan_id=2) must match exactly 1 node");
    assert_eq!(nodes[0].key, "run_2", "matched node must be run_2");

    // NotEq: scan_id != 2 → run_1 and run_3
    let (_, total_neq) = cat
        .search_children(
            None,
            &[Query::NotEq(NotEq {
                key: "scan_id".into(),
                value: json!(2),
            })],
            &[],
            0,
            100,
        )
        .await
        .unwrap();
    assert_eq!(total_neq, 2, "NotEq(scan_id=2) must match 2 nodes");

    // In: scan_id in [1, 3] → run_1 and run_3
    let (nodes_in, total_in) = cat
        .search_children(
            None,
            &[Query::In(In {
                key: "scan_id".into(),
                value: vec![json!(1), json!(3)],
            })],
            &[],
            0,
            100,
        )
        .await
        .unwrap();
    assert_eq!(total_in, 2, "In(scan_id=[1,3]) must match 2 nodes");
    let in_keys: Vec<&str> = nodes_in.iter().map(|n| n.key.as_str()).collect();
    assert!(in_keys.contains(&"run_1") && in_keys.contains(&"run_3"));

    // NotIn: scan_id not in [1, 2] → only run_3
    let (nodes_nin, total_nin) = cat
        .search_children(
            None,
            &[Query::NotIn(NotIn {
                key: "scan_id".into(),
                value: vec![json!(1), json!(2)],
            })],
            &[],
            0,
            100,
        )
        .await
        .unwrap();
    assert_eq!(
        total_nin, 1,
        "NotIn(scan_id=[1,2]) must match exactly 1 node"
    );
    assert_eq!(nodes_nin[0].key, "run_3");

    // Eq on a shared integer value: count == 10 → run_1 and run_3
    let (_, total_count) = cat
        .search_children(
            None,
            &[Query::Eq(Eq {
                key: "count".into(),
                value: json!(10),
            })],
            &[],
            0,
            100,
        )
        .await
        .unwrap();
    assert_eq!(
        total_count, 2,
        "Eq(count=10) must match 2 nodes (run_1 + run_3)"
    );
}

/// F-C: the `sorting` argument drives ORDER BY — default id tiebreaker
/// (insertion order), single metadata key ascending/descending, and the
/// logical "id" key mapping to the `key` (node name) column.
#[tokio::test]
async fn search_children_honors_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let cat = Catalog::connect(&uri).await.unwrap();
    cat.migrate().await.unwrap();

    // Insertion order gamma,alpha,beta so id order differs from name and color.
    for (key, color) in [("gamma", "b"), ("alpha", "c"), ("beta", "a")] {
        cat.create_node(
            None,
            vec![],
            RegisterRequest {
                key: key.into(),
                structure_family: "container".into(),
                metadata: json!({ "color": color }),
                specs: json!([]),
                access_blob: json!({}),
            },
        )
        .await
        .unwrap();
    }

    let names = |nodes: &[tiled_catalog::orm::Node]| -> Vec<String> {
        nodes.iter().map(|n| n.key.clone()).collect()
    };

    // No sort → default id tiebreaker → insertion order.
    let (nodes, _) = cat.search_children(None, &[], &[], 0, 100).await.unwrap();
    assert_eq!(
        names(&nodes),
        vec!["gamma", "alpha", "beta"],
        "no sort → insertion (id) order"
    );

    // Sort by metadata.color ascending → a(beta), b(gamma), c(alpha).
    let asc = [("color".to_string(), SortDirection::Ascending)];
    let (nodes, _) = cat.search_children(None, &[], &asc, 0, 100).await.unwrap();
    assert_eq!(
        names(&nodes),
        vec!["beta", "gamma", "alpha"],
        "color ascending"
    );

    // Sort by metadata.color descending → c(alpha), b(gamma), a(beta).
    let desc = [("color".to_string(), SortDirection::Descending)];
    let (nodes, _) = cat.search_children(None, &[], &desc, 0, 100).await.unwrap();
    assert_eq!(
        names(&nodes),
        vec!["alpha", "gamma", "beta"],
        "color descending"
    );

    // Logical "id" sort key maps to the `key` column (node name).
    let by_name = [("id".to_string(), SortDirection::Ascending)];
    let (nodes, _) = cat
        .search_children(None, &[], &by_name, 0, 100)
        .await
        .unwrap();
    assert_eq!(
        names(&nodes),
        vec!["alpha", "beta", "gamma"],
        "sort 'id' → key column (name) order"
    );
}
