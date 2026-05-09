//! SQLite round-trip — open in-memory, migrate, write, read.

use serde_json::json;

use tiled_catalog::{Catalog, RegisterRequest};
use tiled_catalog::data_source::{AssetSpec, DataSourceSpec};

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

    // Delete cascades.
    cat.delete_node(container.id).await.unwrap();
    assert!(cat.lookup(&["experiment_a".into()]).await.unwrap().is_none());
    let assets_after = cat.list_assets(ds.id).await.unwrap();
    assert_eq!(assets_after.len(), 0);
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
