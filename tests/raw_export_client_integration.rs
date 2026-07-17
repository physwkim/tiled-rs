//! Client-side raw-asset parity: the tiled-rs CLIENT walking a node's backing
//! assets the way upstream's `BaseClient.asset_manifest` / `raw_export` do
//! (`base.py:342` / `:380`).
//!
//! Drives the real client stack (`from_uri_with_options(.., true)` →
//! `ContainerClient::get` → `BaseClient::asset_manifest` / `raw_export`) against
//! a live in-process, SQLite-catalog-backed `tiled-server` on an ephemeral TCP
//! port. Raw-asset download is a catalog capability (a catalog-less server
//! answers `405`), and the assets themselves are external `file://` nodes
//! seeded directly into the catalog (the register path used by `asset_download`
//! HTTP tests). Covers the directory manifest, `raw_export` of a single-file and
//! of a directory asset (bytes + layout), the multi-data-source refusal, and the
//! "no asset id available" guard (client built without `include_data_sources`).

use std::sync::Arc;

use serde_json::json;
use tokio::net::TcpListener;

use tiled_rs::catalog::adapter::UnresolvedLeaf;
use tiled_rs::catalog::data_source::{AssetSpec, DataSourceSpec};
use tiled_rs::catalog::{Catalog, CatalogAdapter, RegisterRequest};
use tiled_rs::client::{
    ClientError, ContextOptions, ExportDestination, from_uri, from_uri_with_options,
};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

/// Build a SQLite-catalog-backed server, spawn it on an ephemeral TCP port, and
/// return `(base_url, catalog, tempdir)`. The returned `Catalog` is a clone
/// sharing the server's DB pool — seed external-asset nodes through it before
/// driving the client. `base_url: None` so the server derives node links from
/// the request `Host`, letting the client follow them back to the ephemeral
/// address. `expose_raw_assets: true` so the asset endpoints are reachable.
async fn spawn_catalog_server() -> (String, Catalog, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> =
        Arc::new(CatalogAdapter::root(catalog.clone(), resolver));
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: None,
        root_path: String::new(),
        cors_policy: tiled_rs::server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: Some(catalog.clone()),
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
        streaming_cache: tiled_rs::server::streaming_cache::disabled(),
        access_policy: None,
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
        enable_web: false,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        webhook_dispatcher: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
        allow_anonymous_access: false,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
        validation: Default::default(),
    };
    let app = tiled_rs::server::build_app(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (base_url, catalog, dir)
}

/// A `DataSourceSpec` for one external `file://` asset (single file or
/// directory). Structure is an arbitrary array structure — the download path
/// never inspects it.
fn one_asset_source(data_uri: &str, is_directory: bool) -> DataSourceSpec {
    DataSourceSpec {
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
    }
}

/// Register a root array node `key` carrying one external asset at `data_uri`.
async fn add_asset(cat: &Catalog, key: &str, data_uri: &str, is_directory: bool) {
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
    cat.create_data_source(node.id, one_asset_source(data_uri, is_directory))
        .await
        .unwrap();
}

/// Register a root array node `key` carrying two external data sources — a node
/// `raw_export` must refuse.
async fn add_two_source_node(cat: &Catalog, key: &str, uri_a: &str, uri_b: &str) {
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
    cat.create_data_source(node.id, one_asset_source(uri_a, false))
        .await
        .unwrap();
    cat.create_data_source(node.id, one_asset_source(uri_b, false))
        .await
        .unwrap();
}

/// A `DataSourceSpec` for one external data source backed by *two* single-file
/// assets (the namespace-by-id `raw_export` path: `len(assets) != 1`). The two
/// assets share a list-valued `parameter` and are ordered by `num`.
fn two_file_asset_source(uri_a: &str, uri_b: &str) -> DataSourceSpec {
    DataSourceSpec {
        structure_family: "array".into(),
        structure: json!({
            "shape": [10],
            "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
            "chunks": [[10]],
        }),
        mimetype: "application/x-hdf5".into(),
        parameters: json!({}),
        management: "external".into(),
        assets: vec![
            AssetSpec {
                data_uri: uri_a.into(),
                is_directory: false,
                parameter: "data_uris".into(),
                num: Some(0),
            },
            AssetSpec {
                data_uri: uri_b.into(),
                is_directory: false,
                parameter: "data_uris".into(),
                num: Some(1),
            },
        ],
    }
}

/// Register a root array node `key` carrying one external data source with two
/// single-file assets (a multi-asset node, namespaced by asset id on export).
async fn add_two_asset_node(cat: &Catalog, key: &str, uri_a: &str, uri_b: &str) {
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
    cat.create_data_source(node.id, two_file_asset_source(uri_a, uri_b))
        .await
        .unwrap();
}

/// A temp directory seeded with `files` (relative posix path → contents). The
/// directory is registered as a single directory asset whose manifest lists
/// every file, giving one download job per file.
fn make_dir(files: &[(&str, &[u8])]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for (rel, contents) in files {
        let target = dir.path().join(rel);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&target, contents).unwrap();
    }
    dir
}

fn file_uri(path: &std::path::Path) -> String {
    tiled_rs::core::file_uri::path_to_file_uri(path).unwrap()
}

/// Fetch node `key` with `include_data_sources=true` and return its
/// `BaseClient` (cloned so it outlives the `AnyClient`).
async fn node_base(base: &str, key: &str) -> tiled_rs::client::BaseClient {
    let root = from_uri_with_options(base, ContextOptions::default(), true)
        .await
        .unwrap()
        .into_container()
        .unwrap();
    let node = root.get(key).await.unwrap();
    node.base().unwrap().clone()
}

#[tokio::test]
async fn asset_manifest_lists_directory_entries_and_none_for_file() {
    let (base, cat, _db) = spawn_catalog_server().await;

    // A directory asset with nested files.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.txt"), b"AAA").unwrap();
    std::fs::create_dir(dir.path().join("sub")).unwrap();
    std::fs::write(dir.path().join("sub").join("b.txt"), b"BBBB").unwrap();
    add_asset(&cat, "frames", &file_uri(dir.path()), true).await;

    let manifest = node_base(&base, "frames")
        .await
        .asset_manifest()
        .await
        .unwrap();
    assert_eq!(manifest.len(), 1);
    assert!(manifest[0].asset_id.is_some());
    assert_eq!(
        manifest[0].relative_paths,
        Some(vec!["a.txt".to_string(), "sub/b.txt".to_string()]),
        "directory manifest lists forward-slash relative paths, sorted"
    );

    // A single-file asset maps to None (no manifest).
    let filedir = tempfile::tempdir().unwrap();
    let file = filedir.path().join("data.bin");
    std::fs::write(&file, b"payload").unwrap();
    add_asset(&cat, "frame", &file_uri(&file), false).await;

    let manifest = node_base(&base, "frame")
        .await
        .asset_manifest()
        .await
        .unwrap();
    assert_eq!(manifest.len(), 1);
    assert_eq!(manifest[0].relative_paths, None);
}

#[tokio::test]
async fn raw_export_single_file_writes_named_file() {
    let (base, cat, _db) = spawn_catalog_server().await;
    let filedir = tempfile::tempdir().unwrap();
    let file = filedir.path().join("scan001.h5");
    let payload = b"\x00\x01\x02\x03hello-asset";
    std::fs::write(&file, payload).unwrap();
    add_asset(&cat, "frame", &file_uri(&file), false).await;

    let dest = tempfile::tempdir().unwrap();
    let written = node_base(&base, "frame")
        .await
        .raw_export(ExportDestination::Directory(dest.path().to_path_buf()), 4)
        .await
        .unwrap()
        .into_paths()
        .expect("directory export returns paths");

    // Single asset → the file lands directly under dest, named after the
    // data_uri basename (== the server's Content-Disposition filename).
    let expected = dest.path().join("scan001.h5");
    assert_eq!(written, vec![expected.clone()]);
    assert_eq!(std::fs::read(&expected).unwrap(), payload);
}

#[tokio::test]
async fn raw_export_directory_writes_relative_layout() {
    let (base, cat, _db) = spawn_catalog_server().await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.txt"), b"AAA").unwrap();
    std::fs::create_dir(dir.path().join("sub")).unwrap();
    std::fs::write(dir.path().join("sub").join("b.txt"), b"BBBB").unwrap();
    add_asset(&cat, "frames", &file_uri(dir.path()), true).await;

    let dest = tempfile::tempdir().unwrap();
    let written = node_base(&base, "frames")
        .await
        .raw_export(ExportDestination::Directory(dest.path().to_path_buf()), 4)
        .await
        .unwrap()
        .into_paths()
        .expect("directory export returns paths");

    // Single asset → files land directly under dest, preserving the relative
    // layout of the manifest (nested `sub/` recreated).
    let a = dest.path().join("a.txt");
    let b = dest.path().join("sub").join("b.txt");
    assert_eq!(written, vec![a.clone(), b.clone()]);
    assert_eq!(std::fs::read(&a).unwrap(), b"AAA");
    assert_eq!(std::fs::read(&b).unwrap(), b"BBBB");
}

#[tokio::test]
async fn raw_export_refuses_multiple_data_sources() {
    let (base, cat, _db) = spawn_catalog_server().await;
    add_two_source_node(&cat, "multi", "file:///tmp/a.h5", "file:///tmp/b.h5").await;

    let dest = tempfile::tempdir().unwrap();
    let err = node_base(&base, "multi")
        .await
        .raw_export(ExportDestination::Directory(dest.path().to_path_buf()), 4)
        .await
        .unwrap_err();
    match err {
        ClientError::Invalid(msg) => {
            assert_eq!(msg, "Export of multiple data sources not yet supported");
        }
        other => panic!("expected Invalid multi-data-source refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn asset_accessors_lazily_fetch_without_include_data_sources() {
    // Client gap #8: a client built WITHOUT include_data_sources no longer
    // errors on the asset accessors — `data_sources()` lazily re-fetches the
    // node's own URI with `?include_data_sources=true` (upstream base.py:299 +
    // :307), so the backing asset ids arrive and the accessors succeed, matching
    // upstream's `self.include_data_sources().data_sources()`.
    let (base, cat, _db) = spawn_catalog_server().await;
    let filedir = tempfile::tempdir().unwrap();
    let file = filedir.path().join("scan.h5");
    let payload = b"lazy-asset-bytes";
    std::fs::write(&file, payload).unwrap();
    add_asset(&cat, "frame", &file_uri(&file), false).await;

    // Plain from_uri → include_data_sources defaults to false.
    let root = from_uri(&base).await.unwrap().into_container().unwrap();
    let node = root.get("frame").await.unwrap();
    let base_client = node.base().unwrap();

    // asset_manifest succeeds via the lazy re-fetch: one single-file asset,
    // carrying a server-assigned id, mapping to no directory manifest.
    let manifest = base_client
        .asset_manifest()
        .await
        .expect("asset_manifest must lazily fetch data sources");
    assert_eq!(manifest.len(), 1);
    assert!(
        manifest[0].asset_id.is_some(),
        "the lazily-fetched asset must carry its server-assigned id"
    );
    assert_eq!(manifest[0].relative_paths, None);

    // raw_export likewise succeeds and writes the file's bytes.
    let dest = tempfile::tempdir().unwrap();
    let written = base_client
        .raw_export(ExportDestination::Directory(dest.path().to_path_buf()), 4)
        .await
        .expect("raw_export must lazily fetch data sources")
        .into_paths()
        .expect("directory export returns paths");
    let expected = dest.path().join("scan.h5");
    assert_eq!(written, vec![expected.clone()]);
    assert_eq!(std::fs::read(&expected).unwrap(), payload);
}

#[tokio::test]
async fn data_sources_lazily_populates_asset_ids_and_matches_eager() {
    // Client gap #8, the accessor itself: `BaseClient::data_sources()` returns
    // the node's data sources with populated asset ids whether or not the client
    // was built with include_data_sources — lazily re-fetching when it was not.
    let (base, cat, _db) = spawn_catalog_server().await;
    let filedir = tempfile::tempdir().unwrap();
    let file = filedir.path().join("data.bin");
    std::fs::write(&file, b"x").unwrap();
    add_asset(&cat, "frame", &file_uri(&file), false).await;

    // Eager client (flag set at construction): sources are already attached.
    let eager = node_base(&base, "frame").await;
    let eager_sources = eager
        .data_sources()
        .await
        .expect("eager data_sources")
        .expect("node has data sources");
    assert_eq!(eager_sources.len(), 1);
    assert_eq!(eager_sources[0].assets.len(), 1);
    let eager_id = eager_sources[0].assets[0]
        .id
        .expect("eager asset carries an id");

    // Lazy client (flag absent): data_sources() must re-fetch and surface the
    // same source, with the same server-assigned asset id.
    let root = from_uri(&base).await.unwrap().into_container().unwrap();
    let lazy = root.get("frame").await.unwrap().base().unwrap().clone();
    let lazy_sources = lazy
        .data_sources()
        .await
        .expect("lazy data_sources must re-fetch, not error")
        .expect("re-fetched node has data sources");
    assert_eq!(lazy_sources.len(), 1);
    assert_eq!(lazy_sources[0].assets.len(), 1);
    assert_eq!(
        lazy_sources[0].assets[0].id,
        Some(eager_id),
        "the lazily-fetched asset id must match the eager fetch"
    );
}

#[tokio::test]
async fn data_sources_none_for_node_without_sources() {
    // A container node carries no data sources; data_sources() returns Ok(None)
    // (upstream returns None for a missing `data_sources` attribute), both eager
    // and lazy — never an error.
    let (base, _cat, _db) = spawn_catalog_server().await;

    // The root container itself has no data sources. Lazy path (from_uri).
    let root_lazy = from_uri(&base).await.unwrap();
    let lazy_base = root_lazy.base().unwrap();
    assert_eq!(
        lazy_base
            .data_sources()
            .await
            .expect("lazy root data_sources"),
        None,
        "a container node reports no data sources (lazy)"
    );

    // Eager path (flag set): same answer, no re-fetch.
    let root_eager = from_uri_with_options(&base, ContextOptions::default(), true)
        .await
        .unwrap();
    let eager_base = root_eager.base().unwrap();
    assert_eq!(
        eager_base
            .data_sources()
            .await
            .expect("eager root data_sources"),
        None,
        "a container node reports no data sources (eager)"
    );
}

// --- parallel + in-memory raw_export boundary cases -------------------------

#[tokio::test]
async fn raw_export_parallel_downloads_every_asset_once() {
    // A directory asset with several files → one download job per file. With
    // `max_workers > 1` they download concurrently; every file must land exactly
    // once (one distinct path each), with its exact bytes.
    let (base, cat, _db) = spawn_catalog_server().await;
    let files: &[(&str, &[u8])] = &[
        ("f0.bin", b"zero"),
        ("f1.bin", b"one-1"),
        ("f2.bin", b"two-22"),
        ("sub/f3.bin", b"three-333"),
        ("sub/deep/f4.bin", b"four-4444"),
    ];
    let dir = make_dir(files);
    add_asset(&cat, "frames", &file_uri(dir.path()), true).await;

    let dest = tempfile::tempdir().unwrap();
    let written = node_base(&base, "frames")
        .await
        .raw_export(ExportDestination::Directory(dest.path().to_path_buf()), 4)
        .await
        .unwrap()
        .into_paths()
        .expect("directory export returns paths");

    // Exactly one path per file, all distinct → each asset downloaded once.
    assert_eq!(written.len(), files.len());
    let distinct: std::collections::BTreeSet<_> = written.iter().cloned().collect();
    assert_eq!(distinct.len(), files.len(), "no path written twice");
    for &(rel, contents) in files {
        let path = dest.path().join(rel);
        assert!(written.contains(&path), "missing {rel} in {written:?}");
        assert_eq!(
            std::fs::read(&path).unwrap().as_slice(),
            contents,
            "bytes for {rel}"
        );
    }
}

#[tokio::test]
async fn raw_export_failing_asset_surfaces_error_without_hanging() {
    // One data source with two single-file assets (namespaced path): the first
    // exists, the second's backing file is deleted before export so its byte
    // fetch fails. The failure must surface as an error, and the pool must
    // return promptly rather than hang — asserted via a bounded timeout.
    let (base, cat, _db) = spawn_catalog_server().await;

    let live_dir = tempfile::tempdir().unwrap();
    let live = live_dir.path().join("present.bin");
    std::fs::write(&live, b"i am here").unwrap();

    let gone_dir = tempfile::tempdir().unwrap();
    let gone = gone_dir.path().join("absent.bin");
    std::fs::write(&gone, b"deleted soon").unwrap();
    let gone_uri = file_uri(&gone);
    std::fs::remove_file(&gone).unwrap();

    add_two_asset_node(&cat, "pair", &file_uri(&live), &gone_uri).await;

    let dest = tempfile::tempdir().unwrap();
    let node = node_base(&base, "pair").await;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        node.raw_export(ExportDestination::Directory(dest.path().to_path_buf()), 4),
    )
    .await
    .expect("raw_export must return, not hang, when a download fails");
    assert!(
        result.is_err(),
        "a failing asset download must surface as an error, got {result:?}"
    );
}

#[tokio::test]
async fn raw_export_memory_sink_matches_disk_bytes() {
    // The in-memory sink must deliver the same bytes the disk sink writes, keyed
    // by the same posix relative paths.
    let (base, cat, _db) = spawn_catalog_server().await;
    let files: &[(&str, &[u8])] = &[
        ("a.txt", b"AAA"),
        ("sub/b.txt", b"BBBB"),
        ("sub/c.txt", b"CCCCC"),
    ];
    let dir = make_dir(files);
    add_asset(&cat, "frames", &file_uri(dir.path()), true).await;

    let dest = tempfile::tempdir().unwrap();
    let disk = node_base(&base, "frames")
        .await
        .raw_export(ExportDestination::Directory(dest.path().to_path_buf()), 3)
        .await
        .unwrap()
        .into_paths()
        .expect("directory export returns paths");

    let mem = node_base(&base, "frames")
        .await
        .raw_export(ExportDestination::Memory, 3)
        .await
        .unwrap()
        .into_memory()
        .expect("memory export returns a map");

    assert_eq!(mem.len(), files.len());
    for &(rel, contents) in files {
        // In-memory bytes keyed by the posix relative path == the source bytes.
        assert_eq!(
            mem.get(rel).map(|b| b.as_ref()),
            Some(contents),
            "memory bytes for {rel}"
        );
        // The disk sink wrote the identical bytes to dest/<rel>.
        let disk_path = dest.path().join(rel);
        assert!(disk.contains(&disk_path), "disk path for {rel}");
        assert_eq!(
            std::fs::read(&disk_path).unwrap().as_slice(),
            contents,
            "disk bytes for {rel}"
        );
    }
}

#[tokio::test]
async fn raw_export_sequential_max_workers_one_writes_all() {
    // `max_workers == 1` runs the jobs one at a time; every file must still be
    // written correctly.
    let (base, cat, _db) = spawn_catalog_server().await;
    let files: &[(&str, &[u8])] = &[("one.bin", b"1"), ("two.bin", b"22"), ("three.bin", b"333")];
    let dir = make_dir(files);
    add_asset(&cat, "frames", &file_uri(dir.path()), true).await;

    let dest = tempfile::tempdir().unwrap();
    let written = node_base(&base, "frames")
        .await
        .raw_export(ExportDestination::Directory(dest.path().to_path_buf()), 1)
        .await
        .unwrap()
        .into_paths()
        .expect("directory export returns paths");

    assert_eq!(written.len(), files.len());
    for &(rel, contents) in files {
        let path = dest.path().join(rel);
        assert!(written.contains(&path), "missing {rel}");
        assert_eq!(std::fs::read(&path).unwrap().as_slice(), contents);
    }
}

#[tokio::test]
async fn raw_export_multiple_assets_namespaced_by_id() {
    // A node backed by two assets namespaces each asset's key/path under its
    // server-assigned id (`{asset_id}/{filename}`), matching upstream's
    // `Path(destination, str(asset.id))` / `f"{asset.id}/{...}"`.
    let (base, cat, _db) = spawn_catalog_server().await;
    let dir_a = tempfile::tempdir().unwrap();
    let a = dir_a.path().join("alpha.bin");
    std::fs::write(&a, b"alpha-bytes").unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let b = dir_b.path().join("beta.bin");
    std::fs::write(&b, b"beta-bytes").unwrap();
    add_two_asset_node(&cat, "pair", &file_uri(&a), &file_uri(&b)).await;

    let node = node_base(&base, "pair").await;
    let sources = node.data_sources().await.unwrap().unwrap();
    let ids: Vec<i64> = sources[0]
        .assets
        .iter()
        .map(|asset| asset.id.expect("eager asset id"))
        .collect();
    assert_eq!(ids.len(), 2);

    let mem = node
        .raw_export(ExportDestination::Memory, 4)
        .await
        .unwrap()
        .into_memory()
        .expect("memory export returns a map");
    assert_eq!(mem.len(), 2);
    assert_eq!(
        mem.get(&format!("{}/alpha.bin", ids[0]))
            .map(|b| b.as_ref()),
        Some(&b"alpha-bytes"[..]),
        "asset 0 namespaced key + bytes"
    );
    assert_eq!(
        mem.get(&format!("{}/beta.bin", ids[1])).map(|b| b.as_ref()),
        Some(&b"beta-bytes"[..]),
        "asset 1 namespaced key + bytes"
    );
}
