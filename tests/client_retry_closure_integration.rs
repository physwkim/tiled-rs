//! Wave-27 retry-closure: extends the transient-failure `retry` wrap to the
//! remaining bare client write/admin/register calls that upstream issues inside
//! `retry_context` — `Container.new` (the POST behind `create_node` and every
//! `write_*` helper, `container.rs`), all five `Admin.*` methods (`admin.rs`),
//! and the three registration server-writes (`register.rs`), which upstream
//! issues via `node.new` → `Container.new` inside `retry_context`.
//!
//! Each mock endpoint returns `503 Service Unavailable` on its first hit and
//! succeeds on the second. A call NOT wrapped in `retry` would surface that
//! first 503 as an error; a wrapped call retries the transient failure and
//! succeeds. Asserting the call returns `Ok` AND the endpoint was hit exactly
//! twice (one failure + one retry) proves the retry is in place. This mirrors
//! upstream, which issues every one of these requests inside `retry_context`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use tiled_rs::client::{ContainerClient, Context, RegisterSettings, register};
use tiled_rs::core::structures::StructureFamily;

/// Per-endpoint hit counter. The first hit for a key serves 503; later hits
/// succeed.
#[derive(Default)]
struct Hits(Mutex<HashMap<String, u32>>);

impl Hits {
    /// Record a hit for `key`. Returns `true` on the first hit (serve 503),
    /// `false` afterwards (serve success).
    async fn first(&self, key: &str) -> bool {
        let mut map = self.0.lock().await;
        let n = map.entry(key.to_string()).or_insert(0);
        *n += 1;
        *n == 1
    }

    async fn count(&self, key: &str) -> u32 {
        *self.0.lock().await.get(key).unwrap_or(&0)
    }
}

async fn spawn(app: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    format!("http://{addr}")
}

fn a_principal(uuid: &str) -> serde_json::Value {
    serde_json::json!({
        "uuid": uuid,
        "type": "service",
        "role": "user",
        "identities": [],
    })
}

// --- container create-node endpoint (container.rs `post_new_node`) ---

async fn create_node(State(hits): State<Arc<Hits>>) -> impl IntoResponse {
    if hits.first("POST /metadata/foo").await {
        return (StatusCode::SERVICE_UNAVAILABLE, "transient").into_response();
    }
    Json(serde_json::json!({ "id": "child" })).into_response()
}

// --- admin endpoints (admin.rs) ---

async fn list_principals(State(hits): State<Arc<Hits>>) -> impl IntoResponse {
    if hits.first("GET /auth/principal").await {
        return (StatusCode::SERVICE_UNAVAILABLE, "transient").into_response();
    }
    Json(serde_json::json!([a_principal("u1")])).into_response()
}

async fn create_service_principal(State(hits): State<Arc<Hits>>) -> impl IntoResponse {
    if hits.first("POST /auth/principal").await {
        return (StatusCode::SERVICE_UNAVAILABLE, "transient").into_response();
    }
    Json(a_principal("u1")).into_response()
}

async fn show_principal(
    State(hits): State<Arc<Hits>>,
    Path(uuid): Path<String>,
) -> impl IntoResponse {
    let key = format!("GET /auth/principal/{uuid}");
    if hits.first(&key).await {
        return (StatusCode::SERVICE_UNAVAILABLE, "transient").into_response();
    }
    Json(a_principal(&uuid)).into_response()
}

async fn create_api_key(
    State(hits): State<Arc<Hits>>,
    Path(uuid): Path<String>,
) -> impl IntoResponse {
    let key = format!("POST /auth/principal/{uuid}/apikey");
    if hits.first(&key).await {
        return (StatusCode::SERVICE_UNAVAILABLE, "transient").into_response();
    }
    Json(serde_json::json!({
        "secret": "sk-full-secret",
        "first_eight": "sk-full-",
        "scopes": [],
        "expiration_time": null,
    }))
    .into_response()
}

async fn revoke_api_key(
    State(hits): State<Arc<Hits>>,
    Path(uuid): Path<String>,
) -> impl IntoResponse {
    let key = format!("DELETE /auth/principal/{uuid}/apikey");
    if hits.first(&key).await {
        return (StatusCode::SERVICE_UNAVAILABLE, "transient").into_response();
    }
    StatusCode::OK.into_response()
}

#[tokio::test]
async fn transient_failure_retries_create_node_and_admin_calls() {
    let hits: Arc<Hits> = Arc::new(Hits::default());
    let app = Router::new()
        .route("/api/v1/metadata/foo", post(create_node))
        .route(
            "/api/v1/auth/principal",
            get(list_principals).post(create_service_principal),
        )
        .route("/api/v1/auth/principal/{uuid}", get(show_principal))
        .route(
            "/api/v1/auth/principal/{uuid}/apikey",
            post(create_api_key).delete(revoke_api_key),
        )
        .with_state(hits.clone());
    let base = spawn(app).await;

    let (ctx, _) = Context::from_uri(&base).unwrap();

    // admin.rs: each of the five methods succeeds despite an initial 503.
    let principals = ctx.admin().list_principals(0, 100).await.unwrap();
    assert_eq!(principals.len(), 1);
    let shown = ctx.admin().show_principal("u1").await.unwrap();
    assert_eq!(shown.uuid, "u1");
    let created_sp = ctx.admin().create_service_principal("user").await.unwrap();
    assert_eq!(created_sp.role, "user");
    let key = ctx
        .admin()
        .create_api_key("u1", None, None, None)
        .await
        .unwrap();
    assert_eq!(key.secret, "sk-full-secret");
    ctx.admin().revoke_api_key("u1", "abcdef12").await.unwrap();

    // container.rs `post_new_node`: build a node whose `self` link is the mock
    // endpoint, then create a child (the POST behind `create_node` and every
    // `write_*` helper).
    let item = serde_json::from_value(serde_json::json!({
        "id": "foo",
        "attributes": { "ancestors": [], "structure_family": "container", "metadata": {} },
        "links": { "self": format!("{base}/api/v1/metadata/foo") }
    }))
    .unwrap();
    let node = ContainerClient::from_item(ctx.clone(), item, false).unwrap();
    let created = node
        .create_node(
            Some("child"),
            StructureFamily::Container,
            serde_json::json!({}),
            vec![],
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(created, "child");

    // Every endpoint saw exactly two requests: the 503 plus the retry. A count
    // of 1 would mean the call was not wrapped in `retry` and failed fast.
    for key in [
        "POST /metadata/foo",
        "GET /auth/principal",
        "POST /auth/principal",
        "GET /auth/principal/u1",
        "POST /auth/principal/u1/apikey",
        "DELETE /auth/principal/u1/apikey",
    ] {
        assert_eq!(hits.count(key).await, 2, "{key} must be retried once");
    }
}

// ---------------------------------------------------------------------------
// register.rs — the three server-writes (`create_container`,
// `register_single_item`, `register_image_sequence`). All three POST to the
// parent container's `/register/` link and are keyed apart by the request body:
// an empty `data_sources` array is the container create; otherwise the first
// data source's `mimetype` (`text/csv` vs `multipart/related;…`) discriminates
// the single-item and image-sequence writes.
// ---------------------------------------------------------------------------

/// Shared state for the register mock: per-site hit counter plus the server's
/// own base URL (echoed into the created child's `self` link).
struct RegState {
    hits: Hits,
    base: String,
}

/// Classify a register POST body into one of the three write sites.
fn register_site(body: &serde_json::Value) -> &'static str {
    match body.get("data_sources").and_then(|v| v.as_array()) {
        Some(sources) if sources.is_empty() => "container",
        Some(sources) => {
            let mimetype = sources
                .first()
                .and_then(|s| s.get("mimetype"))
                .and_then(|m| m.as_str())
                .unwrap_or("");
            if mimetype == "text/csv" {
                "csv"
            } else if mimetype.starts_with("multipart/related") {
                "imgseq"
            } else {
                "other"
            }
        }
        None => "none",
    }
}

async fn register_post(
    State(state): State<Arc<RegState>>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let key = format!("register:{}", register_site(&body));
    if state.hits.first(&key).await {
        return (StatusCode::SERVICE_UNAVAILABLE, "transient").into_response();
    }
    Json(serde_json::json!({ "id": body.get("id").cloned().unwrap_or_default() })).into_response()
}

/// The `create_container` write is followed by `node.get(key)`; serve the child
/// container so the walk can descend into (empty) `subdir` and finish.
async fn get_subdir(State(state): State<Arc<RegState>>) -> impl IntoResponse {
    Json(serde_json::json!({
        "data": {
            "id": "subdir",
            "attributes": { "ancestors": ["root"], "structure_family": "container", "metadata": {} },
            "links": { "self": format!("{}/api/v1/metadata/root/subdir", state.base) }
        }
    }))
    .into_response()
}

#[tokio::test]
async fn transient_failure_retries_register_writes() {
    // A directory whose walk exercises all three register writes: a CSV file
    // (`register_single_item`), a `frame0/frame1.tif` image sequence
    // (`register_image_sequence`), and an empty subdirectory (`create_container`).
    let dir = tempfile::TempDir::new().unwrap();
    let root = dir.path().join("dataset");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("data.csv"), "col1,col2\n1,2\n3,4\n").unwrap();
    std::fs::write(root.join("frame0.tif"), b"").unwrap();
    std::fs::write(root.join("frame1.tif"), b"").unwrap();
    std::fs::create_dir(root.join("subdir")).unwrap();

    // Bind first so the server can echo its own base URL into the child link.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    let state = Arc::new(RegState {
        hits: Hits::default(),
        base: base.clone(),
    });
    let app = Router::new()
        .route("/api/v1/register/root", post(register_post))
        .route("/api/v1/metadata/root/subdir", get(get_subdir))
        .with_state(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (ctx, _) = Context::from_uri(&base).unwrap();
    let item = serde_json::from_value(serde_json::json!({
        "id": "root",
        "attributes": { "ancestors": [], "structure_family": "container", "metadata": {} },
        "links": { "self": format!("{base}/api/v1/metadata/root") }
    }))
    .unwrap();
    let node = ContainerClient::from_item(ctx, item, false).unwrap();

    // The full walk must complete despite an initial 503 on each register write.
    register(&node, &root, "", &RegisterSettings::default(), false)
        .await
        .unwrap();

    // Each register write saw exactly two requests: the 503 plus the retry.
    for key in ["register:imgseq", "register:csv", "register:container"] {
        assert_eq!(state.hits.count(key).await, 2, "{key} must be retried once");
    }
}
