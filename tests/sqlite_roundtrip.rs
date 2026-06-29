//! SQLite round-trip — open in-memory, migrate, write, read.

use serde_json::json;
use tiled_rs::core::queries::{Eq, FullText, In, Like, NotEq, NotIn, Query, SpecsQuery};
use tiled_rs::core::schemas::SortDirection;

use tiled_rs::catalog::data_source::{AssetSpec, DataSourceSpec};
use tiled_rs::catalog::db::DbPool;
use tiled_rs::catalog::{Catalog, RegisterRequest};

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
            "0005_metadata_fts5".to_string(),
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
            None,
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
                // Under the tempdir (never created on disk) so the forced delete
                // below removes nothing real — keeps the test hermetic now that
                // delete reclaims managed file:// assets.
                data_uri: tiled_rs::core::file_uri::path_to_file_uri(&dir.path().join("frame.h5"))
                    .unwrap(),
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
        matches!(err, tiled_rs::catalog::CatalogError::WouldDeleteData(_)),
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

/// Catalog M5: a forced delete (`external_only=false`) reclaims the physical
/// files behind internally-managed `file://` assets — a plain file is unlinked,
/// a directory asset is removed recursively — while external assets' files are
/// left in place. Mirrors Python `delete()` (adapter.py:1188-1191): the
/// `if management != external: delete_physical_asset(...)` loop runs after the
/// rows are gone. Boundaries: management external vs managed, and
/// is_directory false (remove_file) vs true (remove_dir_all).
#[tokio::test]
async fn delete_reclaims_managed_files_keeps_external() {
    use std::fs;

    let dir = tempfile::tempdir().unwrap();
    let cat = Catalog::connect(&format!(
        "sqlite://{}",
        dir.path().join("catalog.db").display()
    ))
    .await
    .unwrap();
    cat.migrate().await.unwrap();

    // Lay down three real assets on disk: a managed file, a managed directory
    // (with a file inside, to exercise recursive removal), and an external file.
    let storage = dir.path().join("storage");
    fs::create_dir_all(&storage).unwrap();
    let managed_file = storage.join("managed.bin");
    fs::write(&managed_file, b"managed-bytes").unwrap();
    let managed_dir = storage.join("managed_dir");
    fs::create_dir_all(&managed_dir).unwrap();
    fs::write(managed_dir.join("part-0"), b"chunk").unwrap();
    let external_file = storage.join("external.bin");
    fs::write(&external_file, b"external-bytes").unwrap();

    let file_uri = |p: &std::path::Path| tiled_rs::core::file_uri::path_to_file_uri(p).unwrap();

    // container/{mfile (writable file), mdir (writable dir), ext (external file)}
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

    let array_req = |key: &str| RegisterRequest {
        key: key.into(),
        structure_family: "array".into(),
        metadata: json!({}),
        specs: json!([]),
        access_blob: json!({}),
    };
    let ds_spec = |management: &str, data_uri: String, is_directory: bool| DataSourceSpec {
        structure_family: "array".into(),
        structure: json!({
            "shape": [1],
            "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
            "chunks": [[1]],
        }),
        mimetype: "application/x-hdf5".into(),
        parameters: json!({}),
        management: management.into(),
        assets: vec![AssetSpec {
            data_uri,
            is_directory,
            parameter: "data_uri".into(),
            num: None,
        }],
    };

    let mfile = cat
        .create_node(Some(container.id), vec!["expt".into()], array_req("mfile"))
        .await
        .unwrap();
    cat.create_data_source(
        mfile.id,
        ds_spec("writable", file_uri(&managed_file), false),
    )
    .await
    .unwrap();
    let mdir = cat
        .create_node(Some(container.id), vec!["expt".into()], array_req("mdir"))
        .await
        .unwrap();
    cat.create_data_source(mdir.id, ds_spec("writable", file_uri(&managed_dir), true))
        .await
        .unwrap();
    let ext = cat
        .create_node(Some(container.id), vec!["expt".into()], array_req("ext"))
        .await
        .unwrap();
    cat.create_data_source(ext.id, ds_spec("external", file_uri(&external_file), false))
        .await
        .unwrap();

    // external_only=true must refuse (managed sources present) and touch nothing.
    let err = cat.delete_node(container.id, true).await.unwrap_err();
    assert!(matches!(
        err,
        tiled_rs::catalog::CatalogError::WouldDeleteData(_)
    ));
    assert!(
        managed_file.exists(),
        "refused delete must not remove files"
    );
    assert!(managed_dir.exists());
    assert!(external_file.exists());

    // Forced delete reclaims the managed files, keeps the external one.
    cat.delete_node(container.id, false).await.unwrap();
    assert!(cat.lookup(&["expt".into()]).await.unwrap().is_none());
    assert!(
        !managed_file.exists(),
        "managed file must be unlinked on forced delete"
    );
    assert!(
        !managed_dir.exists(),
        "managed directory must be removed recursively"
    );
    assert!(
        external_file.exists(),
        "external asset file must be retained (management != external guard)"
    );
}

/// Register a leaf array node under a fresh catalog with a single `writable`
/// (internally-managed) data source pointing at `data_uri`. Returns the node id.
/// Used by the delete-containment boundary tests below.
async fn register_managed_leaf(
    cat: &Catalog,
    key: &str,
    data_uri: String,
    is_directory: bool,
) -> i64 {
    let node = cat
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: key.into(),
                structure_family: "array".into(),
                metadata: json!({}),
                specs: json!([]),
                access_blob: json!({}),
            },
        )
        .await
        .unwrap();
    cat.create_data_source(
        node.id,
        DataSourceSpec {
            structure_family: "array".into(),
            structure: json!({
                "shape": [1],
                "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
                "chunks": [[1]],
            }),
            mimetype: "application/x-hdf5".into(),
            parameters: json!({}),
            management: "writable".into(),
            assets: vec![AssetSpec {
                data_uri,
                is_directory,
                parameter: "data_uri".into(),
                num: None,
            }],
        },
    )
    .await
    .unwrap();
    node.id
}

/// S2 (delete-side path traversal): with a `Restricted` delete scope, a forced
/// delete REFUSES to remove a managed asset whose `data_uri` resolves OUTSIDE
/// the allowed directories — a client can register a managed (default
/// `writable`) data source with an arbitrary `file://` path, so the delete must
/// not become an arbitrary-file delete. The out-of-storage file must survive.
#[tokio::test]
async fn restricted_delete_refuses_managed_asset_outside_allowed_dirs() {
    use std::fs;
    let dir = tempfile::tempdir().unwrap();
    let allowed = dir.path().join("storage");
    fs::create_dir_all(&allowed).unwrap();
    let outside = dir.path().join("outside");
    fs::create_dir_all(&outside).unwrap();
    let victim = outside.join("victim.bin");
    fs::write(&victim, b"do-not-delete").unwrap();

    let cat = Catalog::connect(&format!("sqlite://{}", dir.path().join("c.db").display()))
        .await
        .unwrap()
        .with_managed_delete_dirs(vec![allowed.clone()]);
    cat.migrate().await.unwrap();
    let node_id = register_managed_leaf(
        &cat,
        "leaf",
        tiled_rs::core::file_uri::path_to_file_uri(&victim).unwrap(),
        false,
    )
    .await;

    let err = cat.delete_node(node_id, false).await.unwrap_err();
    assert!(
        matches!(err, tiled_rs::catalog::CatalogError::Validation(ref m) if m.contains("outside the configured managed-delete")),
        "out-of-storage managed delete must be refused, got {err:?}"
    );
    assert!(
        victim.exists(),
        "a managed asset outside storage must NOT be deleted"
    );
}

/// Counterpart: a managed asset whose file lives UNDER an allowed directory is
/// reclaimed normally — containment permits in-storage paths.
#[tokio::test]
async fn restricted_delete_allows_managed_asset_inside_allowed_dirs() {
    use std::fs;
    let dir = tempfile::tempdir().unwrap();
    let allowed = dir.path().join("storage");
    fs::create_dir_all(&allowed).unwrap();
    let managed = allowed.join("managed.bin");
    fs::write(&managed, b"bytes").unwrap();

    let cat = Catalog::connect(&format!("sqlite://{}", dir.path().join("c.db").display()))
        .await
        .unwrap()
        .with_managed_delete_dirs(vec![allowed.clone()]);
    cat.migrate().await.unwrap();
    let node_id = register_managed_leaf(
        &cat,
        "leaf",
        tiled_rs::core::file_uri::path_to_file_uri(&managed).unwrap(),
        false,
    )
    .await;

    cat.delete_node(node_id, false).await.unwrap();
    assert!(
        !managed.exists(),
        "a managed asset inside storage must be reclaimed on forced delete"
    );
}

/// Deny-by-default: an empty `Restricted` dir list refuses removal of any
/// EXISTING managed file (mirrors the read-side empty allow-list serving
/// nothing). The file must survive and the delete must error.
#[tokio::test]
async fn restricted_empty_dirs_denies_managed_delete() {
    use std::fs;
    let dir = tempfile::tempdir().unwrap();
    let victim = dir.path().join("victim.bin");
    fs::write(&victim, b"do-not-delete").unwrap();

    let cat = Catalog::connect(&format!("sqlite://{}", dir.path().join("c.db").display()))
        .await
        .unwrap()
        .with_managed_delete_dirs(vec![]);
    cat.migrate().await.unwrap();
    let node_id = register_managed_leaf(
        &cat,
        "leaf",
        tiled_rs::core::file_uri::path_to_file_uri(&victim).unwrap(),
        false,
    )
    .await;

    let err = cat.delete_node(node_id, false).await.unwrap_err();
    assert!(
        matches!(err, tiled_rs::catalog::CatalogError::Validation(ref m) if m.contains("no managed-delete directory")),
        "empty Restricted scope must deny all managed deletes, got {err:?}"
    );
    assert!(victim.exists(), "deny-all must not remove the file");
}

/// Boundary: a managed asset whose backing file is ALREADY GONE is skipped, not
/// an error, even under deny-all — `resolve_contained_target` returns `Ok(None)`
/// for `NotFound` before the containment decision (you cannot destroy what is
/// absent), so the node delete still succeeds.
#[tokio::test]
async fn restricted_delete_skips_absent_managed_file() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("never-created.bin");

    let cat = Catalog::connect(&format!("sqlite://{}", dir.path().join("c.db").display()))
        .await
        .unwrap()
        .with_managed_delete_dirs(vec![]);
    cat.migrate().await.unwrap();
    let node_id = register_managed_leaf(
        &cat,
        "leaf",
        tiled_rs::core::file_uri::path_to_file_uri(&missing).unwrap(),
        false,
    )
    .await;

    cat.delete_node(node_id, false).await.unwrap();
    assert!(
        cat.lookup(&["leaf".into()]).await.unwrap().is_none(),
        "node with an already-absent managed file must still delete cleanly"
    );
}

/// S2 source side: `validate_managed_data_uri` (the write/register gate) refuses
/// a managed `file://` data_uri resolving OUTSIDE the allowed dirs, even though
/// the target does not exist yet — the fail-fast counterpart of the delete-time
/// check so an out-of-storage managed asset can never be registered.
#[tokio::test]
async fn write_validate_refuses_managed_uri_outside_allowed_dirs() {
    use std::fs;
    let dir = tempfile::tempdir().unwrap();
    let allowed = dir.path().join("storage");
    fs::create_dir_all(&allowed).unwrap();
    let outside = dir.path().join("outside");
    fs::create_dir_all(&outside).unwrap();

    let cat = Catalog::connect("sqlite::memory:")
        .await
        .unwrap()
        .with_managed_delete_dirs(vec![allowed]);

    // Not-yet-created leaf under an out-of-storage dir.
    let uri = tiled_rs::core::file_uri::path_to_file_uri(&outside.join("new.bin")).unwrap();
    let err = cat.validate_managed_data_uri(&uri).unwrap_err();
    assert!(
        matches!(err, tiled_rs::catalog::CatalogError::Validation(ref m) if m.contains("outside the configured storage")),
        "out-of-storage managed register must be refused, got {err:?}"
    );
}

/// Counterpart: a managed data_uri UNDER an allowed dir validates even when the
/// file (and an intermediate dir) does not exist yet — registration precedes
/// the write, so existence must not be required.
#[tokio::test]
async fn write_validate_allows_managed_uri_inside_allowed_dirs() {
    use std::fs;
    let dir = tempfile::tempdir().unwrap();
    let allowed = dir.path().join("storage");
    fs::create_dir_all(&allowed).unwrap();

    let cat = Catalog::connect("sqlite::memory:")
        .await
        .unwrap()
        .with_managed_delete_dirs(vec![allowed.clone()]);

    let uri = tiled_rs::core::file_uri::path_to_file_uri(&allowed.join("sub/new.bin")).unwrap();
    cat.validate_managed_data_uri(&uri)
        .expect("in-storage not-yet-created managed asset must validate");
}

/// A managed `data_uri` that uses `..` to climb out of the allow-list is
/// refused. The shared cross-platform parser ([`tiled_rs::core::file_uri`])
/// canonicalizes the URI per RFC 3986, collapsing `..` segments, so
/// `<allowed>/../secret.bin` resolves to a sibling of `allowed` and is caught by
/// the containment gate (rather than by `resolve_write_target`'s literal `..`
/// check, which a normalized path can no longer reach). The security property —
/// the escape is refused — is unchanged; only the failing gate differs.
#[tokio::test]
async fn write_validate_rejects_parent_dir_escape() {
    use std::fs;
    let dir = tempfile::tempdir().unwrap();
    let allowed = dir.path().join("storage");
    fs::create_dir_all(&allowed).unwrap();

    let cat = Catalog::connect("sqlite::memory:")
        .await
        .unwrap()
        .with_managed_delete_dirs(vec![allowed.clone()]);

    let uri = tiled_rs::core::file_uri::path_to_file_uri(&allowed.join("../secret.bin")).unwrap();
    let err = cat.validate_managed_data_uri(&uri).unwrap_err();
    assert!(
        matches!(err, tiled_rs::catalog::CatalogError::Validation(ref m) if m.contains("outside the configured storage directories")),
        "a parent-dir escape must be refused, got {err:?}"
    );
}

/// The default `Unrestricted` scope (bare/embedded catalog) accepts any managed
/// data_uri — write-time containment is opt-in, mirroring deletion.
#[tokio::test]
async fn write_validate_unrestricted_accepts_anything() {
    let cat = Catalog::connect("sqlite::memory:").await.unwrap();
    cat.validate_managed_data_uri("file:///anywhere/at/all.bin")
        .expect("Unrestricted scope must accept any managed data_uri");
}

/// Deny-by-default: an empty `Restricted` dir list refuses every managed
/// data_uri at register time (same posture as the empty-list delete deny).
#[tokio::test]
async fn write_validate_empty_dirs_denies() {
    let cat = Catalog::connect("sqlite::memory:")
        .await
        .unwrap()
        .with_managed_delete_dirs(vec![]);
    // The data_uri must decode to a real absolute file path on the host
    // platform, else it is treated as non-managed and accepted unchanged. A
    // drive-less `/some/...` is absolute on Unix but not on Windows.
    #[cfg(unix)]
    let managed_uri = "file:///some/where.bin";
    #[cfg(windows)]
    let managed_uri = "file:///C:/some/where.bin";
    let err = cat.validate_managed_data_uri(managed_uri).unwrap_err();
    assert!(
        matches!(err, tiled_rs::catalog::CatalogError::Validation(_)),
        "empty Restricted scope must deny managed register, got {err:?}"
    );
}

/// A non-`file://` URI (e.g. an sqlite storage URI) is not a managed filesystem
/// path and is never a physical-delete target, so it is accepted unchanged.
#[tokio::test]
async fn write_validate_non_file_uri_accepted() {
    let cat = Catalog::connect("sqlite::memory:")
        .await
        .unwrap()
        .with_managed_delete_dirs(vec![]);
    cat.validate_managed_data_uri("sqlite:///var/lib/data.db")
        .expect("non-file managed URI is not an fs path and must be accepted");
}

/// Server M3: `asset_by_id` is node-scoped — an asset id resolves only via the
/// path of the node that owns it, so a foreign asset id returns None (404 at the
/// HTTP layer), never another node's files.
#[tokio::test]
async fn asset_by_id_is_node_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let cat = Catalog::connect(&uri).await.unwrap();
    cat.migrate().await.unwrap();

    let make_array = |key: &str| RegisterRequest {
        key: key.into(),
        structure_family: "array".into(),
        metadata: json!({}),
        specs: json!([]),
        access_blob: json!({}),
    };
    let array_a = cat
        .create_node(None, vec![], make_array("a"))
        .await
        .unwrap();
    let array_b = cat
        .create_node(None, vec![], make_array("b"))
        .await
        .unwrap();

    let ds_spec = |data_uri: &str, is_directory: bool| DataSourceSpec {
        structure_family: "array".into(),
        structure: json!({
            "shape": [10],
            "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
            "chunks": [[10]],
        }),
        mimetype: "application/x-hdf5".into(),
        parameters: json!({}),
        management: "external".into(),
        assets: vec![AssetSpec {
            data_uri: data_uri.into(),
            is_directory,
            parameter: "data_uri".into(),
            num: None,
        }],
    };
    let ds_a = cat
        .create_data_source(array_a.id, ds_spec("file:///tmp/a.h5", false))
        .await
        .unwrap();
    cat.create_data_source(array_b.id, ds_spec("file:///tmp/b_dir", true))
        .await
        .unwrap();

    let asset_a = cat.list_assets(ds_a.id).await.unwrap().remove(0);

    // Own node resolves the asset.
    let got = cat.asset_by_id(array_a.id, asset_a.id).await.unwrap();
    assert!(got.is_some(), "owning node must resolve its asset");
    assert_eq!(got.unwrap().data_uri, "file:///tmp/a.h5");

    // The OTHER node must NOT resolve node a's asset id, even though it exists.
    assert!(
        cat.asset_by_id(array_b.id, asset_a.id)
            .await
            .unwrap()
            .is_none(),
        "asset_by_id must be node-scoped: a foreign asset id returns None"
    );

    // A nonexistent asset id returns None.
    assert!(
        cat.asset_by_id(array_a.id, 999_999)
            .await
            .unwrap()
            .is_none(),
        "nonexistent asset id returns None"
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
    assert!(matches!(
        err,
        tiled_rs::catalog::CatalogError::Validation(_)
    ));
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
    assert!(matches!(err, tiled_rs::catalog::CatalogError::Conflict(_)));
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

    let names = |nodes: &[tiled_rs::catalog::orm::Node]| -> Vec<String> {
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

/// FTS5 full-text search (catalog M2). Migration 0005 builds an external-content
/// FTS5 index over `nodes.metadata` kept in sync by the AFTER INSERT/UPDATE/DELETE
/// triggers. `FullText` must match whole tokens (case-insensitively), NOT `LIKE`
/// substrings — the behaviour the old `metadata LIKE %term%` path got wrong.
/// Mirrors Python `metadata_fts5.c.metadata.match(text)` (adapter.py:2014).
#[tokio::test]
async fn full_text_search_uses_fts5_token_match() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let cat = Catalog::connect(&uri).await.unwrap();
    cat.migrate().await.unwrap();

    let parent = cat
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: "runs".into(),
                structure_family: "container".into(),
                metadata: json!({}),
                specs: json!([]),
                access_blob: json!({}),
            },
        )
        .await
        .unwrap();

    let child = |key: &str, meta: serde_json::Value| RegisterRequest {
        key: key.into(),
        structure_family: "array".into(),
        metadata: meta,
        specs: json!([]),
        access_blob: json!({}),
    };

    // AFTER INSERT trigger populates the index for these two children.
    let a = cat
        .create_node(
            Some(parent.id),
            vec!["runs".into()],
            child("a", json!({"material": "copper oxide", "scan": "alpha"})),
        )
        .await
        .unwrap();
    let b = cat
        .create_node(
            Some(parent.id),
            vec!["runs".into()],
            child("b", json!({"material": "iron", "scan": "beta"})),
        )
        .await
        .unwrap();

    // Collect the matching child keys (sorted) for a FullText query.
    async fn hits(cat: &Catalog, parent_id: i64, text: &str) -> Vec<String> {
        let (nodes, total) = cat
            .search_children(
                Some(parent_id),
                &[Query::FullText(FullText { text: text.into() })],
                &[],
                0,
                100,
            )
            .await
            .unwrap();
        assert_eq!(
            total as usize,
            nodes.len(),
            "total must equal page len here"
        );
        let mut keys: Vec<String> = nodes.into_iter().map(|n| n.key).collect();
        keys.sort();
        keys
    }

    // Whole-token match, and case-insensitive (FTS5 unicode61 folds case).
    assert_eq!(hits(&cat, parent.id, "copper").await, vec!["a"]);
    assert_eq!(hits(&cat, parent.id, "COPPER").await, vec!["a"]);
    assert_eq!(hits(&cat, parent.id, "oxide").await, vec!["a"]);
    assert_eq!(hits(&cat, parent.id, "iron").await, vec!["b"]);

    // Token match, NOT substring: a token prefix does not match. The old
    // `LIKE %copp%` path would have (wrongly) returned "a" here.
    assert!(
        hits(&cat, parent.id, "copp").await.is_empty(),
        "FTS5 token match must not match a bare token prefix"
    );
    // Absent token → no match.
    assert!(hits(&cat, parent.id, "zirconium").await.is_empty());

    // AFTER UPDATE trigger re-indexes: change b's metadata from iron → copper.
    cat.update_metadata(
        b.id,
        json!({"material": "copper foil", "scan": "beta"}),
        json!([]),
        None,
        /* drop_revision */ false,
    )
    .await
    .unwrap();
    assert_eq!(
        hits(&cat, parent.id, "copper").await,
        vec!["a", "b"],
        "update trigger must add b to the copper results"
    );
    assert!(
        hits(&cat, parent.id, "iron").await.is_empty(),
        "update trigger must drop b's old 'iron' token"
    );

    // AFTER DELETE trigger removes a from the index (a is an array with no
    // data source, so the external_only safety gate permits the delete).
    cat.delete_node(a.id, true).await.unwrap();
    assert_eq!(
        hits(&cat, parent.id, "copper").await,
        vec!["b"],
        "delete trigger must drop a from the copper results"
    );
}

/// `connect_with_pool_size(n)` threads `n` through to `PoolOptions::max_connections`.
/// With max_connections=1, holding one connection makes `try_acquire()` return None
/// because the single slot is occupied.
#[tokio::test]
async fn connect_with_pool_size_limits_connections() {
    // In-memory SQLite — no tempdir needed.
    let cat = tiled_rs::catalog::Catalog::connect_with_pool_size("sqlite:", 1)
        .await
        .unwrap();
    cat.migrate().await.unwrap();

    // Acquire the one allowed connection.
    let DbPool::Sqlite(pool) = cat.pool() else {
        panic!("expected SQLite pool in SQLite test");
    };
    let _conn1 = pool.acquire().await.unwrap();

    // With conn1 held and max_connections=1, a second try_acquire must return None.
    // This is the load-bearing assertion: it proves max_connections reached the builder.
    assert!(
        pool.try_acquire().is_none(),
        "pool size 1: try_acquire must return None while one connection is held"
    );
}
