//! End-to-end test for `tiled_rs::client::smoke_read` — the port of
//! `tiled/client/smoke.py`'s `read`, a recursive whole-tree health check.
//!
//! A single SQLite-catalog-backed `tiled-server` with a writable-storage root is
//! spawned on an ephemeral TCP port (so managed array/table writes persist to
//! disk). Trees are built with the client `write_*` helpers, then walked with
//! `smoke_read`. A leaf is "broken" by deleting its backing file after
//! registration — the exact scenario the health check exists to catch — via the
//! leaf's own data-source asset URIs.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use tokio::net::TcpListener;

use tiled_rs::catalog::Catalog;
use tiled_rs::client::{
    ContainerClient, ContextOptions, from_uri, from_uri_with_options, smoke_read,
};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
use tiled_rs::core::queries::Query;
use tiled_rs::core::structures::ArrayStructure;
use tiled_rs::server::file_resolver::FileLeafResolver;

/// Build a SQLite-catalog-backed server with a configured writable-storage root,
/// spawn it on an ephemeral TCP port, and return `(base_url, writable_dir,
/// db_dir)`. Both tempdirs are kept alive by the caller. `base_url: None` so the
/// server derives node links from the request `Host`, letting the client follow
/// them back to the real ephemeral address.
async fn spawn_write_server() -> (String, tempfile::TempDir, tempfile::TempDir) {
    let db_dir = tempfile::tempdir().unwrap();
    let writable_dir = tempfile::tempdir().unwrap();
    let writable_root = writable_dir.path().canonicalize().unwrap();

    let db_uri = format!("sqlite://{}", db_dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&db_uri)
        .await
        .unwrap()
        .with_managed_delete_dirs(vec![writable_root.clone()])
        .with_writable_storage(vec![writable_root.clone()]);
    catalog.migrate().await.unwrap();

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> =
        Arc::new(FileLeafResolver::new(vec![writable_root.clone()]));
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));

    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: Arc::new(tiled_rs::serialization::default_registry()),
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: None,
        root_path: String::new(),
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
    (base_url, writable_dir, db_dir)
}

// --- fixtures -------------------------------------------------------------

fn f64_le(vals: &[f64]) -> bytes::Bytes {
    vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn array_structure(shape: usize) -> ArrayStructure {
    ArrayStructure {
        data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
        chunks: vec![vec![shape]],
        shape: vec![shape],
        dims: None,
        resizable: Default::default(),
    }
}

fn tbl_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("y", DataType::Utf8, false),
    ]))
}

fn tbl_batch() -> RecordBatch {
    RecordBatch::try_new(
        tbl_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap()
}

/// Write a healthy managed array leaf `key` under `parent`.
async fn write_array_leaf(parent: &ContainerClient, key: &str, shape: usize, vals: &[f64]) {
    parent
        .write_array(
            Some(key),
            array_structure(shape),
            f64_le(vals),
            serde_json::json!({}),
            vec![],
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("write array {key}: {e:?}"));
}

/// Navigate `path` from the root, then delete every backing file of the leaf it
/// names (making its `read()` fail). Returns the leaf's `self` URI so the caller
/// can assert `smoke_read` reports exactly it. Uses an `include_data_sources`
/// client so the leaf's asset URIs are populated.
async fn break_leaf(base: &str, path: &[&str]) -> String {
    let mut node = from_uri_with_options(base, ContextOptions::default(), true)
        .await
        .unwrap();
    for seg in path {
        let container = node.into_container().expect("intermediate container");
        node = container.get(seg).await.expect("navigate to leaf");
    }
    let bc = node.base().expect("leaf has a base client");
    let uri = bc.uri().expect("leaf self uri").to_string();
    let sources = bc
        .data_sources()
        .await
        .expect("leaf data sources")
        .expect("managed leaf has data sources");
    for ds in &sources {
        for asset in &ds.assets {
            let p = tiled_rs::core::file_uri::file_uri_to_path(&asset.data_uri)
                .expect("asset data_uri -> path");
            if asset.is_directory {
                let _ = std::fs::remove_dir_all(&p);
            } else {
                let _ = std::fs::remove_file(&p);
            }
        }
    }
    uri
}

// --- tests ----------------------------------------------------------------

#[tokio::test]
async fn smoke_healthy_tree_returns_empty() {
    let (base, _wd, _db) = spawn_write_server().await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    // Array leaf, table leaf, and a nested container with its own array — every
    // leaf readable.
    write_array_leaf(&root, "arr", 4, &[1.0, 2.0, 3.0, 4.0]).await;
    root.write_table(
        Some("tbl"),
        &tbl_schema(),
        &[tbl_batch()],
        serde_json::json!({}),
        vec![],
        None,
    )
    .await
    .expect("write tbl");
    let grp = root
        .create_container(Some("grp"), serde_json::json!({}))
        .await
        .expect("create grp");
    write_array_leaf(&grp, "inner", 2, &[10.0, 20.0]).await;

    let any = from_uri(&base).await.unwrap();
    let faulty = smoke_read(&any).await.expect("smoke walk");
    assert!(
        faulty.is_empty(),
        "a fully readable tree must report no faults, got {faulty:?}"
    );
}

#[tokio::test]
async fn smoke_reports_broken_leaf_with_uri() {
    let (base, _wd, _db) = spawn_write_server().await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    // A healthy sibling and a leaf whose backing file is deleted after write.
    write_array_leaf(&root, "healthy", 4, &[1.0, 2.0, 3.0, 4.0]).await;
    write_array_leaf(&root, "broken", 4, &[5.0, 6.0, 7.0, 8.0]).await;

    let broken_uri = break_leaf(&base, &["broken"]).await;

    let any = from_uri(&base).await.unwrap();
    let faulty = smoke_read(&any).await.expect("smoke walk");

    // Exactly the broken leaf is reported, by its URI; the healthy sibling is not.
    assert_eq!(
        faulty.len(),
        1,
        "exactly one faulty leaf expected, got {faulty:?}"
    );
    assert_eq!(
        faulty[0].uri, broken_uri,
        "the reported URI is the broken leaf"
    );
    assert!(
        faulty.iter().all(|f| !f.uri.is_empty()),
        "a faulty leaf carries its URI"
    );
}

#[tokio::test]
async fn smoke_walks_nested_containers() {
    let (base, _wd, _db) = spawn_write_server().await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    // A broken leaf buried two containers deep, beside a healthy nested leaf.
    let grp = root
        .create_container(Some("grp"), serde_json::json!({}))
        .await
        .expect("create grp");
    let sub = grp
        .create_container(Some("sub"), serde_json::json!({}))
        .await
        .expect("create grp/sub");
    write_array_leaf(&sub, "ok", 2, &[1.0, 2.0]).await;
    write_array_leaf(&sub, "bad", 2, &[3.0, 4.0]).await;

    let bad_uri = break_leaf(&base, &["grp", "sub", "bad"]).await;

    let any = from_uri(&base).await.unwrap();
    let faulty = smoke_read(&any).await.expect("smoke walk");

    // The walk recursed into grp/sub and found the deeply-nested broken leaf,
    // while the healthy nested sibling read cleanly.
    assert_eq!(
        faulty.len(),
        1,
        "exactly the nested broken leaf expected, got {faulty:?}"
    );
    assert_eq!(faulty[0].uri, bad_uri, "the nested broken leaf's URI");
}
