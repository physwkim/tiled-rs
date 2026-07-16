//! Client-side metadata-revisions parity: the tiled-rs CLIENT walking a node's
//! revision history the way upstream's `MetadataRevisions` does
//! (`client.metadata_revisions[i]` / `len()` / `delete_revision(n)`).
//!
//! Drives the real client stack (`from_uri` → `ContainerClient::create_container`
//! → `BaseClient::patch_metadata` → `BaseClient::revisions`) against a live
//! in-process, SQLite-catalog-backed `tiled-server` on an ephemeral TCP port
//! (revisions are a catalog capability — a catalog-less server answers `405`).
//! Covers the total count, offset/limit pagination (limit=1 walks all pages via
//! `links.next`), single-revision content, deleting a revision, and the two
//! error paths (delete-nonexistent, get-out-of-range).

use std::sync::Arc;

use tokio::net::TcpListener;

use tiled_rs::catalog::{Catalog, adapter::UnresolvedLeaf};
use tiled_rs::client::{ClientError, ContainerClient, PatchContentType, from_uri};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

/// Build a SQLite-catalog-backed server, spawn it on an ephemeral TCP port, and
/// return `(base_url, tempdir)`. The tempdir (holding the `.db` file) is kept
/// alive by the caller for the duration of the requests. `base_url: None` so
/// the server derives node links from the request `Host`, letting the client
/// follow them back to the real ephemeral address.
async fn spawn_catalog_server() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: None,
        cors_policy: tiled_rs::server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: Some(catalog),
        auth_db: None,
        issuer: None,
        authenticators: vec![],
        proxied_header_auth: None,
        external_oidc: None,
        #[cfg(feature = "saml")]
        saml_providers: vec![],
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_rs::server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
        enable_web: false,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    };
    let app = tiled_rs::server::build_app(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (base_url, dir)
}

/// Create container `node` with metadata `{"a": 1}`, then merge-patch it to
/// `{"a": 2}` and `{"a": 3}`. Each patch records the *pre-patch* state, so this
/// leaves two revisions: number 1 = `{"a": 1}` and number 2 = `{"a": 2}`
/// (`next_revision = MAX(revision) + 1`, so numbering is 1-based ascending).
async fn make_node_with_two_revisions(base: &str) -> ContainerClient {
    let root = from_uri(base).await.unwrap().into_container().unwrap();
    let node = root
        .create_container(Some("node"), serde_json::json!({"a": 1}))
        .await
        .unwrap();
    node.base()
        .patch_metadata(
            Some(serde_json::json!({"a": 2})),
            None,
            None,
            PatchContentType::MergePatch,
            false,
        )
        .await
        .unwrap();
    node.base()
        .patch_metadata(
            Some(serde_json::json!({"a": 3})),
            None,
            None,
            PatchContentType::MergePatch,
            false,
        )
        .await
        .unwrap();
    node
}

#[tokio::test]
async fn revisions_count_list_and_get() {
    let (base, _dir) = spawn_catalog_server().await;
    let node = make_node_with_two_revisions(&base).await;
    let revs = node.base().revisions().unwrap();

    // Total count is the node-wide total (post-#1409), not a page length.
    assert_eq!(revs.count().await.unwrap(), 2);

    // limit=1 forces the client to walk both pages via `links.next`.
    let all = revs.list(0, Some(1)).await.unwrap();
    assert_eq!(all.len(), 2, "limit=1 must still return every revision");
    assert_eq!(all[0].revision_number, 1);
    assert_eq!(all[1].revision_number, 2);
    assert_eq!(all[0].metadata, serde_json::json!({"a": 1}));
    assert_eq!(all[1].metadata, serde_json::json!({"a": 2}));

    // get(offset) is offset-based, oldest-first (offset 0 == the oldest).
    let first = revs.get(0).await.unwrap();
    assert_eq!(first.revision_number, 1);
    assert_eq!(first.metadata, serde_json::json!({"a": 1}));
    let second = revs.get(1).await.unwrap();
    assert_eq!(second.revision_number, 2);
    assert_eq!(second.metadata, serde_json::json!({"a": 2}));
}

#[tokio::test]
async fn revisions_delete_drops_and_relists() {
    let (base, _dir) = spawn_catalog_server().await;
    let node = make_node_with_two_revisions(&base).await;
    let revs = node.base().revisions().unwrap();
    assert_eq!(revs.count().await.unwrap(), 2);

    // Delete revision number 1 (the oldest). Count drops and the fresh listing
    // reflects the removal, leaving only revision 2.
    revs.delete(1).await.unwrap();
    assert_eq!(revs.count().await.unwrap(), 1);
    let remaining = revs.list(0, None).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].revision_number, 2);
    assert_eq!(remaining[0].metadata, serde_json::json!({"a": 2}));
}

#[tokio::test]
async fn revisions_delete_nonexistent_is_error() {
    let (base, _dir) = spawn_catalog_server().await;
    let node = make_node_with_two_revisions(&base).await;
    let revs = node.base().revisions().unwrap();

    let err = revs.delete(999).await.unwrap_err();
    assert!(
        matches!(err, ClientError::Server { status: 404, .. }),
        "deleting a nonexistent revision must surface the server 404, got {err:?}"
    );
}

#[tokio::test]
async fn revisions_get_out_of_range_is_key_not_found() {
    let (base, _dir) = spawn_catalog_server().await;
    let node = make_node_with_two_revisions(&base).await;
    let revs = node.base().revisions().unwrap();

    // Offset past the end returns an empty page → KeyNotFound (mirrors
    // upstream `__getitem__`'s `(result,) = content["data"]` ValueError).
    let err = revs.get(5).await.unwrap_err();
    assert!(
        matches!(err, ClientError::KeyNotFound(_)),
        "an offset past the end must be KeyNotFound, got {err:?}"
    );
}
