//! `?max_depth=` validation and `structure.contents` inlining on the metadata
//! and search endpoints — full parity with upstream tiled
//! (`tiled/server/router.py:322,460`, `tiled/server/core.py:468-563`).
//!
//! Upstream types the query param as `Query(None, ge=0, le=DEPTH_LIMIT)` on BOTH
//! routes, so an out-of-range or non-integer value is a 422 before the handler
//! body runs; a valid value threads into `construct_resource` /
//! `construct_entries_response`, where the gate
//! `((max_depth is None) or (depth < max_depth)) and
//! inlined_contents_enabled(depth) and depth <= DEPTH_LIMIT` decides whether a
//! container's children are inlined into `structure.contents`.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use indexmap::IndexMap;
use serde_json::{Value, json};
use tokio::net::TcpListener;

use tiled_rs::adapters::{ArrayAdapter, MapAdapter};
use tiled_rs::core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_rs::core::dtype::{BuiltinDType, Endianness, Kind};
use tiled_rs::core::queries::Query;
use tiled_rs::core::structures::Spec;

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

/// A 1-D `f64` array leaf carrying `metadata`.
fn arr(data: &[f64], metadata: Value) -> AnyAdapter {
    let bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
    let dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
    let a = ArrayAdapter::from_array(
        Bytes::from(bytes),
        dtype,
        vec![data.len()],
        vec![vec![data.len()]],
        metadata,
        vec![],
    );
    AnyAdapter::Array(Arc::new(a))
}

/// A container carrying `specs` (empty = plain container).
fn container(children: Vec<(&str, AnyAdapter)>, specs: Vec<Spec>, metadata: Value) -> AnyAdapter {
    let mut m = IndexMap::new();
    for (k, v) in children {
        m.insert(k.to_string(), v);
    }
    AnyAdapter::Container(Arc::new(MapAdapter::new(m, metadata, specs)))
}

/// The test tree:
/// ```text
/// root
/// ├── ds     (container, spec "xarray_dataset")   → inlining-enabled
/// │   ├── x  (array)
/// │   └── y  (array)
/// ├── plain  (container, no spec)                  → contents stays None
/// │   └── a  (array)
/// └── leaf   (array)
/// ```
fn build_root() -> Arc<dyn ContainerAdapter> {
    let ds = container(
        vec![
            ("x", arr(&[1.0, 2.0, 3.0], json!({"units": "K"}))),
            ("y", arr(&[4.0, 5.0, 6.0], json!({}))),
        ],
        vec![Spec::new("xarray_dataset")],
        json!({"kind": "dataset"}),
    );
    let plain = container(vec![("a", arr(&[7.0, 8.0], json!({})))], vec![], json!({}));
    let mut m = IndexMap::new();
    m.insert("ds".to_string(), ds);
    m.insert("plain".to_string(), plain);
    m.insert("leaf".to_string(), arr(&[0.0], json!({})));
    Arc::new(MapAdapter::new(m, json!({}), vec![]))
}

async fn spawn(root: Arc<dyn ContainerAdapter>) -> String {
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree: root,
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
        catalog: None,
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
    let base = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    base
}

/// GET `url`, returning `(status, parsed-json-body)`.
async fn get_json(url: &str) -> (u16, Value) {
    let resp = reqwest::Client::new().get(url).send().await.unwrap();
    let status = resp.status().as_u16();
    let body: Value = resp.json().await.unwrap();
    (status, body)
}

// ---------------------------------------------------------------------------
// Commit 1: ?max_depth= parse + validation (Query(None, ge=0, le=DEPTH_LIMIT)).
//
// Boundaries (pydantic v2 message parity):
//   absent          → 200 (None)
//   0               → 200 (lower bound)
//   5 (= DEPTH_LIMIT) → 200 (upper bound)
//   6               → 422 "Input should be less than or equal to 5"
//   -1              → 422 "Input should be greater than or equal to 0"
//   abc             → 422 "Input should be a valid integer, unable to parse
//                          string as an integer"
// Applied identically on /metadata and /search.
// ---------------------------------------------------------------------------

fn error_message(body: &Value) -> String {
    body.get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_string()
}

#[tokio::test]
async fn metadata_max_depth_absent_is_200() {
    let base = spawn(build_root()).await;
    let (status, _) = get_json(&format!("{base}/api/v1/metadata/")).await;
    assert_eq!(status, 200, "absent max_depth must serve normally");
}

#[tokio::test]
async fn metadata_max_depth_zero_is_200() {
    let base = spawn(build_root()).await;
    let (status, _) = get_json(&format!("{base}/api/v1/metadata/?max_depth=0")).await;
    assert_eq!(status, 200, "max_depth=0 is the valid lower bound");
}

#[tokio::test]
async fn metadata_max_depth_five_is_200() {
    let base = spawn(build_root()).await;
    let (status, _) = get_json(&format!("{base}/api/v1/metadata/?max_depth=5")).await;
    assert_eq!(
        status, 200,
        "max_depth=5 (= DEPTH_LIMIT) is the valid upper bound"
    );
}

#[tokio::test]
async fn metadata_max_depth_six_is_422() {
    let base = spawn(build_root()).await;
    let (status, body) = get_json(&format!("{base}/api/v1/metadata/?max_depth=6")).await;
    assert_eq!(status, 422, "max_depth=6 exceeds DEPTH_LIMIT");
    assert_eq!(
        error_message(&body),
        "Input should be less than or equal to 5"
    );
}

#[tokio::test]
async fn metadata_max_depth_negative_is_422() {
    let base = spawn(build_root()).await;
    let (status, body) = get_json(&format!("{base}/api/v1/metadata/?max_depth=-1")).await;
    assert_eq!(status, 422, "negative max_depth violates ge=0");
    assert_eq!(
        error_message(&body),
        "Input should be greater than or equal to 0"
    );
}

#[tokio::test]
async fn metadata_max_depth_non_integer_is_422() {
    let base = spawn(build_root()).await;
    let (status, body) = get_json(&format!("{base}/api/v1/metadata/?max_depth=abc")).await;
    assert_eq!(status, 422, "non-integer max_depth cannot parse");
    assert_eq!(
        error_message(&body),
        "Input should be a valid integer, unable to parse string as an integer"
    );
}

#[tokio::test]
async fn search_max_depth_absent_is_200() {
    let base = spawn(build_root()).await;
    let (status, _) = get_json(&format!("{base}/api/v1/search/")).await;
    assert_eq!(status, 200, "absent max_depth must serve normally");
}

#[tokio::test]
async fn search_max_depth_zero_is_200() {
    let base = spawn(build_root()).await;
    let (status, _) = get_json(&format!("{base}/api/v1/search/?max_depth=0")).await;
    assert_eq!(status, 200, "max_depth=0 is the valid lower bound");
}

#[tokio::test]
async fn search_max_depth_five_is_200() {
    let base = spawn(build_root()).await;
    let (status, _) = get_json(&format!("{base}/api/v1/search/?max_depth=5")).await;
    assert_eq!(
        status, 200,
        "max_depth=5 (= DEPTH_LIMIT) is the valid upper bound"
    );
}

#[tokio::test]
async fn search_max_depth_six_is_422() {
    let base = spawn(build_root()).await;
    let (status, body) = get_json(&format!("{base}/api/v1/search/?max_depth=6")).await;
    assert_eq!(status, 422, "max_depth=6 exceeds DEPTH_LIMIT");
    assert_eq!(
        error_message(&body),
        "Input should be less than or equal to 5"
    );
}

#[tokio::test]
async fn search_max_depth_negative_is_422() {
    let base = spawn(build_root()).await;
    let (status, body) = get_json(&format!("{base}/api/v1/search/?max_depth=-1")).await;
    assert_eq!(status, 422, "negative max_depth violates ge=0");
    assert_eq!(
        error_message(&body),
        "Input should be greater than or equal to 0"
    );
}

#[tokio::test]
async fn search_max_depth_non_integer_is_422() {
    let base = spawn(build_root()).await;
    let (status, body) = get_json(&format!("{base}/api/v1/search/?max_depth=abc")).await;
    assert_eq!(status, 422, "non-integer max_depth cannot parse");
    assert_eq!(
        error_message(&body),
        "Input should be a valid integer, unable to parse string as an integer"
    );
}

// ---------------------------------------------------------------------------
// Commit 3: recursive structure.contents inlining under the upstream gate
//   ((max_depth is None) or (depth < max_depth))
//   && inlined_contents_enabled(depth) && depth <= DEPTH_LIMIT
// (tiled/server/core.py:513-556). The addressed node is depth 0.
//
// Nested inline-enabled tree:
//   root
//   ├── outer  (xarray_dataset)                depth 0
//   │   ├── leaf0 (array)                        depth 1
//   │   └── inner (xarray_dataset)               depth 1
//   │       └── leaf1 (array)                     depth 2
//   └── plain  (no spec)  → contents null
//       └── a  (array)
// ---------------------------------------------------------------------------

fn build_nested() -> Arc<dyn ContainerAdapter> {
    let inner = container(
        vec![("leaf1", arr(&[1.0], json!({})))],
        vec![Spec::new("xarray_dataset")],
        json!({}),
    );
    let outer = container(
        vec![("leaf0", arr(&[2.0], json!({}))), ("inner", inner)],
        vec![Spec::new("xarray_dataset")],
        json!({}),
    );
    let plain = container(vec![("a", arr(&[3.0], json!({})))], vec![], json!({}));
    let mut m = IndexMap::new();
    m.insert("outer".to_string(), outer);
    m.insert("plain".to_string(), plain);
    Arc::new(MapAdapter::new(m, json!({}), vec![]))
}

/// `structure` object of the metadata response for `path`.
async fn meta_structure(base: &str, path: &str) -> Value {
    let (status, body) = get_json(&format!("{base}/api/v1/metadata/{path}")).await;
    assert_eq!(status, 200, "metadata/{path} must be 200");
    body["data"]["attributes"]["structure"].clone()
}

// A plain (unspec'd) container never inlines — hasattr-absent parity — at any
// max_depth, including the inline-active default (absent). count is still set.
#[tokio::test]
async fn metadata_plain_container_contents_null() {
    let base = spawn(build_nested()).await;
    for q in ["", "?max_depth=5", "?max_depth=1"] {
        let s = meta_structure(&base, &format!("plain{q}")).await;
        assert!(
            s["contents"].is_null(),
            "plain container must keep contents=null (q={q}): {s}"
        );
        assert_eq!(s["count"], 1, "plain container count");
    }
}

// max_depth=0 disables inlining even on an enabled node (0 < 0 is false).
#[tokio::test]
async fn metadata_max_depth_zero_no_inline() {
    let base = spawn(build_nested()).await;
    let s = meta_structure(&base, "outer?max_depth=0").await;
    assert!(s["contents"].is_null(), "max_depth=0 must not inline: {s}");
    assert_eq!(s["count"], 2, "count still reported");
}

// max_depth=1: the depth-0 node inlines its children, but each child container
// (depth 1) does NOT recurse (1 < 1 is false) — its own contents stay null.
#[tokio::test]
async fn metadata_max_depth_one_inlines_one_level_only() {
    let base = spawn(build_nested()).await;
    let s = meta_structure(&base, "outer?max_depth=1").await;
    // outer inlined: both children present.
    assert_eq!(s["count"], 2);
    assert_eq!(s["contents"]["leaf0"]["id"], "leaf0");
    assert_eq!(
        s["contents"]["leaf0"]["attributes"]["structure_family"],
        "array"
    );
    // inner is inlined as a Resource, but its OWN contents do not recurse.
    assert_eq!(s["contents"]["inner"]["id"], "inner");
    assert_eq!(
        s["contents"]["inner"]["attributes"]["structure_family"],
        "container"
    );
    assert!(
        s["contents"]["inner"]["attributes"]["structure"]["contents"].is_null(),
        "depth-1 child must not recurse at max_depth=1: {s}"
    );
    assert_eq!(
        s["contents"]["inner"]["attributes"]["structure"]["count"], 1,
        "inner still reports its child count"
    );
}

// max_depth absent (None) inlines recursively down to the DEPTH_LIMIT bound:
// outer (depth 0) → inner (depth 1) → leaf1 (depth 2).
#[tokio::test]
async fn metadata_max_depth_none_inlines_recursively() {
    let base = spawn(build_nested()).await;
    let s = meta_structure(&base, "outer").await;
    assert_eq!(s["count"], 2);
    // Recurse two levels: inner is inlined AND its child leaf1 is inlined.
    let inner_structure = &s["contents"]["inner"]["attributes"]["structure"];
    assert_eq!(inner_structure["count"], 1);
    assert_eq!(inner_structure["contents"]["leaf1"]["id"], "leaf1");
    assert_eq!(
        inner_structure["contents"]["leaf1"]["attributes"]["structure_family"],
        "array"
    );
}

// max_depth=2 reaches the same full depth as None for this 2-level tree.
#[tokio::test]
async fn metadata_max_depth_two_reaches_leaf1() {
    let base = spawn(build_nested()).await;
    let s = meta_structure(&base, "outer?max_depth=2").await;
    let inner_structure = &s["contents"]["inner"]["attributes"]["structure"];
    assert_eq!(inner_structure["contents"]["leaf1"]["id"], "leaf1");
}

// A large xarray_dataset: exactly INLINED_CONTENTS_LIMIT (500) children inline;
// 501 exceeds the cap so contents stays null (count still reported).
fn big_dataset(n: usize) -> AnyAdapter {
    let mut children = IndexMap::new();
    for i in 0..n {
        children.insert(format!("v{i:04}"), arr(&[i as f64], json!({})));
    }
    AnyAdapter::Container(Arc::new(MapAdapter::new(
        children,
        json!({}),
        vec![Spec::new("xarray_dataset")],
    )))
}

fn root_with_child(key: &str, child: AnyAdapter) -> Arc<dyn ContainerAdapter> {
    let mut m = IndexMap::new();
    m.insert(key.to_string(), child);
    Arc::new(MapAdapter::new(m, json!({}), vec![]))
}

#[tokio::test]
async fn metadata_inline_count_cap_500_inlines() {
    let base = spawn(root_with_child("ds", big_dataset(500))).await;
    let s = meta_structure(&base, "ds").await;
    assert_eq!(s["count"], 500, "exactly-at-cap count");
    assert_eq!(
        s["contents"].as_object().map(|o| o.len()),
        Some(500),
        "500 children (== cap) are inlined"
    );
}

#[tokio::test]
async fn metadata_inline_count_cap_501_truncates() {
    let base = spawn(root_with_child("ds", big_dataset(501))).await;
    let s = meta_structure(&base, "ds").await;
    assert!(
        s["contents"].is_null(),
        "501 children (> cap) must not inline"
    );
    assert_eq!(
        s["count"], 501,
        "count still reported when too large to inline"
    );
}

// ---------------------------------------------------------------------------
// Search-path inlining: each entry is built at depth 0, so an inline-enabled
// container entry inlines its children into structure.contents (unless
// ?max_depth=0). A plain container entry stays contents=null.
// ---------------------------------------------------------------------------

/// The entry with `id == key` from a search response's `data` list.
fn find_entry<'a>(body: &'a Value, key: &str) -> &'a Value {
    body["data"]
        .as_array()
        .expect("search data is a list")
        .iter()
        .find(|e| e["id"] == key)
        .unwrap_or_else(|| panic!("no search entry {key}"))
}

#[tokio::test]
async fn search_inlines_enabled_container_entry() {
    let base = spawn(build_nested()).await;
    // Search the root: entries are `outer` and `plain`.
    let (status, body) = get_json(&format!("{base}/api/v1/search/")).await;
    assert_eq!(status, 200);
    let outer = find_entry(&body, "outer");
    let s = &outer["attributes"]["structure"];
    // outer inlines its children (depth 0, absent max_depth), and inner recurses.
    assert_eq!(s["contents"]["leaf0"]["id"], "leaf0");
    assert_eq!(
        s["contents"]["inner"]["attributes"]["structure"]["contents"]["leaf1"]["id"],
        "leaf1"
    );
    // The plain sibling entry stays non-inlined.
    let plain = find_entry(&body, "plain");
    assert!(
        plain["attributes"]["structure"]["contents"].is_null(),
        "plain container entry must not inline"
    );
}

#[tokio::test]
async fn search_max_depth_zero_no_inline() {
    let base = spawn(build_nested()).await;
    let (status, body) = get_json(&format!("{base}/api/v1/search/?max_depth=0")).await;
    assert_eq!(status, 200);
    let outer = find_entry(&body, "outer");
    assert!(
        outer["attributes"]["structure"]["contents"].is_null(),
        "search max_depth=0 must not inline"
    );
    assert_eq!(outer["attributes"]["structure"]["count"], 2);
}

// ---------------------------------------------------------------------------
// Commit 4: POST /container/full forwards ?max_depth= (closes the
// GET-honors/POST-drops asymmetry of the zip/hdf5 export port extension). The
// zip export caps recursion at `current_depth >= max_depth`, emitting a
// truncation crumb instead of descending — so max_depth is byte-observable.
//
// Tree: root → outer_grp → inner_grp → leaf. Exporting `outer_grp` as zip:
//   max_depth=0 → inner_grp is a truncation crumb (no leaf)
//   max_depth=1 → recurse into inner_grp, leaf included
// ---------------------------------------------------------------------------

fn build_zip_tree() -> Arc<dyn ContainerAdapter> {
    let inner = container(
        vec![("leaf", arr(&[1.0, 2.0], json!({})))],
        vec![],
        json!({}),
    );
    let outer = container(vec![("inner_grp", inner)], vec![], json!({}));
    root_with_child("outer_grp", outer)
}

async fn get_bytes(url: &str) -> (u16, Vec<u8>) {
    let resp = reqwest::Client::new().get(url).send().await.unwrap();
    let status = resp.status().as_u16();
    (status, resp.bytes().await.unwrap().to_vec())
}

/// POST with no body (mirrors `post_container_full` with an empty field list).
async fn post_bytes(url: &str) -> (u16, Vec<u8>) {
    let resp = reqwest::Client::new().post(url).send().await.unwrap();
    let status = resp.status().as_u16();
    (status, resp.bytes().await.unwrap().to_vec())
}

/// POST a `LongRequest` JSON body (the `/container/full` no-path route).
async fn post_json_bytes(url: &str, body: Value) -> (u16, Vec<u8>) {
    let resp = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    (status, resp.bytes().await.unwrap().to_vec())
}

/// Parse an in-memory zip archive into `(entry name, decompressed bytes)`
/// pairs, sorted by name. Two archives produced by separate requests get
/// independent DOS mod-time stamps in their local-file and central-directory
/// headers (2-second granularity), so a raw-byte compare of the two archives
/// is a timing flake when the requests straddle a boundary. Comparing this
/// structural view instead asserts the real claim: same entry set, same
/// per-entry contents.
fn zip_entries(bytes: &[u8]) -> Vec<(String, Vec<u8>)> {
    use std::io::Read;
    let mut archive =
        zip::ZipArchive::new(std::io::Cursor::new(bytes.to_vec())).expect("valid zip archive");
    let mut entries: Vec<(String, Vec<u8>)> = (0..archive.len())
        .map(|i| {
            let mut file = archive.by_index(i).unwrap();
            let name = file.name().to_string();
            let mut content = Vec::new();
            file.read_to_end(&mut content).unwrap();
            (name, content)
        })
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

// Sanity: max_depth is byte-observable on the GET zip export (crumb vs leaf), so
// the POST-equals-GET assertions below actually test forwarding, not a no-op.
#[tokio::test]
async fn zip_export_max_depth_is_observable_on_get() {
    let base = spawn(build_zip_tree()).await;
    let (s0, g0) = get_bytes(&format!(
        "{base}/api/v1/container/full/outer_grp?format=zip&max_depth=0"
    ))
    .await;
    let (s1, g1) = get_bytes(&format!(
        "{base}/api/v1/container/full/outer_grp?format=zip&max_depth=1"
    ))
    .await;
    assert_eq!((s0, s1), (200, 200), "both zip exports serve 200");
    assert_ne!(g0, g1, "max_depth must change the zip export");
}

// post_container_full (the `/container/full/{path}` POST) must forward max_depth
// from the query it reconstructs, so POST(max_depth=N) == GET(?max_depth=N).
#[tokio::test]
async fn post_path_forwards_max_depth() {
    let base = spawn(build_zip_tree()).await;
    let (_, get0) = get_bytes(&format!(
        "{base}/api/v1/container/full/outer_grp?format=zip&max_depth=0"
    ))
    .await;
    let (_, get1) = get_bytes(&format!(
        "{base}/api/v1/container/full/outer_grp?format=zip&max_depth=1"
    ))
    .await;
    let (ps0, post0) = post_bytes(&format!(
        "{base}/api/v1/container/full/outer_grp?format=zip&max_depth=0"
    ))
    .await;
    let (ps1, post1) = post_bytes(&format!(
        "{base}/api/v1/container/full/outer_grp?format=zip&max_depth=1"
    ))
    .await;
    assert_eq!((ps0, ps1), (200, 200));
    assert_eq!(
        zip_entries(&post0),
        zip_entries(&get0),
        "POST max_depth=0 must equal GET max_depth=0"
    );
    assert_eq!(
        zip_entries(&post1),
        zip_entries(&get1),
        "POST max_depth=1 must equal GET max_depth=1"
    );
    assert_ne!(post0, post1, "POST must actually honor max_depth");
}

// container_full_post (the `/container/full` no-path LongRequest route) must
// forward max_depth from the JSON body via LongRequest::to_query_params.
#[tokio::test]
async fn post_longrequest_forwards_max_depth() {
    let base = spawn(build_zip_tree()).await;
    let (_, get0) = get_bytes(&format!(
        "{base}/api/v1/container/full/outer_grp?format=zip&max_depth=0"
    ))
    .await;
    let (_, get1) = get_bytes(&format!(
        "{base}/api/v1/container/full/outer_grp?format=zip&max_depth=1"
    ))
    .await;
    let (ps0, post0) = post_json_bytes(
        &format!("{base}/api/v1/container/full"),
        json!({"path": "outer_grp", "format": "zip", "max_depth": 0}),
    )
    .await;
    let (ps1, post1) = post_json_bytes(
        &format!("{base}/api/v1/container/full"),
        json!({"path": "outer_grp", "format": "zip", "max_depth": 1}),
    )
    .await;
    assert_eq!((ps0, ps1), (200, 200));
    assert_eq!(
        zip_entries(&post0),
        zip_entries(&get0),
        "LongRequest max_depth=0 must equal GET max_depth=0"
    );
    assert_eq!(
        zip_entries(&post1),
        zip_entries(&get1),
        "LongRequest max_depth=1 must equal GET max_depth=1"
    );
    assert_ne!(post0, post1, "LongRequest must actually honor max_depth");
}
