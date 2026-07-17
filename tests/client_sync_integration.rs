//! End-to-end test for `tiled_rs::client::copy` — the port of
//! `tiled/client/sync.py`'s `copy()`.
//!
//! Two independent, SQLite-catalog-backed `tiled-server`s are spawned on
//! ephemeral TCP ports, each with its own writable-storage root (so managed
//! array/table/sparse writes actually persist). A source tree holding an array,
//! a table (with a spec), a nested container with its own array, and a sparse
//! array is built on the first server via the client `write_*` helpers, then
//! `copy`'d to a container on the second server. Every leaf is read back from the
//! destination and asserted equal — data, metadata, and specs.
//!
//! Separate cases pin the three `OnConflict` policies against a destination that
//! already holds a colliding key:
//! * `Error` — the copy fails and the existing entry is left untouched.
//! * `Skip` — the existing entry survives; its siblings are still copied.
//! * `Warn` — same observable outcome as `Skip` (skip-with-log), matching
//!   upstream `warnings.warn(...)` + `continue`.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use tokio::net::TcpListener;

use tiled_rs::catalog::Catalog;
use tiled_rs::client::{ContainerClient, OnConflict, copy, from_uri};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
use tiled_rs::core::queries::Query;
use tiled_rs::core::structures::{ArrayStructure, SparseStructure};
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

// --- expected data --------------------------------------------------------

const ARR_VALUES: [f64; 4] = [1.5, 2.5, 3.5, 4.5];
const INNER_VALUES: [f64; 2] = [10.0, 20.0];
const TBL_X: [i64; 3] = [1, 2, 3];
const TBL_Y: [&str; 3] = ["a", "b", "c"];
// Sparse 3x3 with non-zeros at (0,1)=1.5 and (2,0)=3.7.
const SPARSE_COORD0: [i64; 2] = [0, 2];
const SPARSE_COORD1: [i64; 2] = [1, 0];
const SPARSE_DATA: [f64; 2] = [1.5, 3.7];

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
            Arc::new(Int64Array::from(TBL_X.to_vec())),
            Arc::new(StringArray::from(TBL_Y.to_vec())),
        ],
    )
    .unwrap()
}

/// Build the full source tree under `root`:
/// `arr` (array), `tbl` (table + spec), `grp/inner` (nested array), `spa` (sparse).
async fn build_source_tree(root: &ContainerClient) {
    root.write_array(
        Some("arr"),
        array_structure(4),
        f64_le(&ARR_VALUES),
        serde_json::json!({"kind": "array", "n": 4}),
        vec![],
        None,
    )
    .await
    .expect("write source arr");

    root.write_table(
        Some("tbl"),
        &tbl_schema(),
        &[tbl_batch()],
        serde_json::json!({"kind": "table"}),
        vec![serde_json::json!({"name": "xy_table"})],
        None,
    )
    .await
    .expect("write source tbl");

    let grp = root
        .create_container(Some("grp"), serde_json::json!({"kind": "group"}))
        .await
        .expect("create source grp");
    grp.write_array(
        Some("inner"),
        array_structure(2),
        f64_le(&INNER_VALUES),
        serde_json::json!({"kind": "inner"}),
        vec![],
        None,
    )
    .await
    .expect("write source grp/inner");

    root.write_sparse(
        Some("spa"),
        SparseStructure {
            chunks: vec![vec![3], vec![3]],
            shape: vec![3, 3],
            data_type: Some(DType::Builtin(BuiltinDType::new(
                Endianness::Little,
                Kind::Float,
                8,
            ))),
            ..Default::default()
        },
        (
            &[SPARSE_COORD0.to_vec(), SPARSE_COORD1.to_vec()],
            &SPARSE_DATA,
        ),
        serde_json::json!({"kind": "sparse"}),
        vec![],
        None,
    )
    .await
    .expect("write source spa");
}

// --- readback helpers -----------------------------------------------------

async fn read_array_values(root: &ContainerClient, path: &str) -> Vec<f64> {
    let arr = root.get(path).await.unwrap().into_array().unwrap();
    let blocks = arr.read().await.unwrap();
    blocks
        .iter()
        .flat_map(|b| {
            b.data
                .chunks_exact(8)
                .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        })
        .collect()
}

/// Sorted `((dim0, dim1), value)` tuples, so equality does not depend on the
/// server's COO row order.
fn sparse_sorted(coords: &[Vec<i64>], data: &[f64]) -> Vec<((i64, i64), u64)> {
    let mut out: Vec<((i64, i64), u64)> = (0..data.len())
        .map(|i| ((coords[0][i], coords[1][i]), data[i].to_bits()))
        .collect();
    out.sort();
    out
}

// --- tests ----------------------------------------------------------------

#[tokio::test]
async fn copy_tree_replicates_every_family() {
    let (src_base, _swd, _sdb) = spawn_write_server().await;
    let (dst_base, _dwd, _ddb) = spawn_write_server().await;

    let src = from_uri(&src_base).await.unwrap().into_container().unwrap();
    let dst = from_uri(&dst_base).await.unwrap().into_container().unwrap();
    build_source_tree(&src).await;

    copy(&src, &dst, OnConflict::Error)
        .await
        .expect("copy tree");

    // Array leaf: data + metadata.
    assert_eq!(read_array_values(&dst, "arr").await, ARR_VALUES.to_vec());
    let arr = dst.get("arr").await.unwrap();
    assert_eq!(
        arr.base().unwrap().metadata(),
        &serde_json::json!({"kind": "array", "n": 4}),
        "arr metadata must travel with the copy"
    );

    // Table leaf: data + metadata + spec.
    let tbl = dst.get("tbl").await.unwrap().into_table().unwrap();
    let part = tbl.read_partition(0, None).await.unwrap();
    let batch = &part.batches[0];
    let x = batch
        .column_by_name("x")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let y = batch
        .column_by_name("y")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(x.values(), TBL_X.as_slice(), "tbl x column");
    assert_eq!(
        (0..y.len()).map(|i| y.value(i)).collect::<Vec<_>>(),
        TBL_Y.to_vec(),
        "tbl y column"
    );
    assert_eq!(
        tbl.base().metadata(),
        &serde_json::json!({"kind": "table"}),
        "tbl metadata"
    );
    assert!(
        tbl.base().specs().iter().any(|s| s.name == "xy_table"),
        "tbl spec 'xy_table' must travel with the copy, got {:?}",
        tbl.base().specs()
    );

    // Nested container + its array leaf.
    let grp = dst.get("grp").await.unwrap();
    assert_eq!(
        grp.base().unwrap().metadata(),
        &serde_json::json!({"kind": "group"}),
        "grp metadata"
    );
    let grp_c = grp.into_container().unwrap();
    assert_eq!(
        read_array_values(&grp_c, "inner").await,
        INNER_VALUES.to_vec(),
        "nested grp/inner data"
    );

    // Sparse leaf: data + metadata.
    let spa = dst.get("spa").await.unwrap().into_sparse().unwrap();
    let block = spa.read().await.unwrap();
    assert_eq!(
        sparse_sorted(&block.coords, &block.data),
        sparse_sorted(
            &[SPARSE_COORD0.to_vec(), SPARSE_COORD1.to_vec()],
            &SPARSE_DATA
        ),
        "sparse COO data"
    );
    assert_eq!(
        spa.base().metadata(),
        &serde_json::json!({"kind": "sparse"}),
        "spa metadata"
    );
}

/// Seed the destination with a colliding `arr` (shape [2], values [99, 99]) so a
/// copy of the source's `arr` (shape [4]) would clobber it if it did not honor
/// the conflict policy.
async fn seed_colliding_arr(dst: &ContainerClient) {
    dst.write_array(
        Some("arr"),
        array_structure(2),
        f64_le(&[99.0, 99.0]),
        serde_json::json!({"seeded": true}),
        vec![],
        None,
    )
    .await
    .expect("seed dst arr");
}

#[tokio::test]
async fn on_conflict_error_fails_and_preserves_existing() {
    let (src_base, _swd, _sdb) = spawn_write_server().await;
    let (dst_base, _dwd, _ddb) = spawn_write_server().await;

    let src = from_uri(&src_base).await.unwrap().into_container().unwrap();
    let dst = from_uri(&dst_base).await.unwrap().into_container().unwrap();
    build_source_tree(&src).await;
    seed_colliding_arr(&dst).await;

    let result = copy(&src, &dst, OnConflict::Error).await;
    assert!(
        result.is_err(),
        "OnConflict::Error must fail on the colliding key"
    );

    // The pre-existing entry is untouched — same shape [2], same values,
    // same metadata — never overwritten by the source's shape-[4] array.
    assert_eq!(
        read_array_values(&dst, "arr").await,
        vec![99.0, 99.0],
        "existing arr must not be clobbered"
    );
    assert_eq!(
        dst.get("arr").await.unwrap().base().unwrap().metadata(),
        &serde_json::json!({"seeded": true}),
        "existing arr metadata must not be clobbered"
    );
}

#[tokio::test]
async fn on_conflict_skip_preserves_existing_and_copies_siblings() {
    let (src_base, _swd, _sdb) = spawn_write_server().await;
    let (dst_base, _dwd, _ddb) = spawn_write_server().await;

    let src = from_uri(&src_base).await.unwrap().into_container().unwrap();
    let dst = from_uri(&dst_base).await.unwrap().into_container().unwrap();
    build_source_tree(&src).await;
    seed_colliding_arr(&dst).await;

    copy(&src, &dst, OnConflict::Skip).await.expect("skip copy");

    // Existing `arr` survives untouched.
    assert_eq!(
        read_array_values(&dst, "arr").await,
        vec![99.0, 99.0],
        "skipped arr must survive"
    );
    assert_eq!(
        dst.get("arr").await.unwrap().base().unwrap().metadata(),
        &serde_json::json!({"seeded": true}),
        "skipped arr metadata must survive"
    );

    // Siblings are still copied.
    let tbl = dst.get("tbl").await.unwrap().into_table().unwrap();
    let part = tbl.read_partition(0, None).await.unwrap();
    let x = part.batches[0]
        .column_by_name("x")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        x.values(),
        TBL_X.as_slice(),
        "sibling tbl copied under skip"
    );
    assert_eq!(
        read_array_values(
            &dst.get("grp").await.unwrap().into_container().unwrap(),
            "inner"
        )
        .await,
        INNER_VALUES.to_vec(),
        "sibling grp/inner copied under skip"
    );
}

#[tokio::test]
async fn on_conflict_warn_skips_existing_like_skip() {
    let (src_base, _swd, _sdb) = spawn_write_server().await;
    let (dst_base, _dwd, _ddb) = spawn_write_server().await;

    let src = from_uri(&src_base).await.unwrap().into_container().unwrap();
    let dst = from_uri(&dst_base).await.unwrap().into_container().unwrap();
    build_source_tree(&src).await;
    seed_colliding_arr(&dst).await;

    // Warn is skip-with-log: it must not error, and it must not overwrite.
    copy(&src, &dst, OnConflict::Warn).await.expect("warn copy");

    assert_eq!(
        read_array_values(&dst, "arr").await,
        vec![99.0, 99.0],
        "warn must skip (not overwrite) the existing arr"
    );
    // Siblings still copied.
    let tbl = dst.get("tbl").await.unwrap().into_table().unwrap();
    assert_eq!(
        tbl.read_partition(0, None).await.unwrap().batches[0]
            .column_by_name("x")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        TBL_X.as_slice(),
        "sibling tbl copied under warn"
    );
}
