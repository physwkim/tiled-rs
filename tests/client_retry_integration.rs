//! Wave-27 batch-3, Finding 1: transient-failure `retry` now wraps the
//! metadata-write (`base.rs`) and auth-management (`context.rs`) client calls,
//! not just reads and data-block writes.
//!
//! Each mock endpoint returns `503 Service Unavailable` on its first hit and
//! succeeds on the second. A call that is NOT wrapped in `retry` would surface
//! that first 503 as an error; a wrapped call retries the transient failure and
//! succeeds. Asserting the call returns `Ok` AND the endpoint was hit exactly
//! twice (one failure + one retry) proves the retry is in place. This mirrors
//! upstream, which issues every one of these requests inside `retry_context`.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::{Method, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{delete, get, patch, post};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use tiled_rs::client::{ContainerClient, Context};

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

// --- auth-management endpoints (context.rs) ---

async fn create_api_key(State(hits): State<Arc<Hits>>) -> impl IntoResponse {
    if hits.first("POST /auth/apikeys").await {
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

async fn which_api_key(State(hits): State<Arc<Hits>>) -> impl IntoResponse {
    if hits.first("GET /auth/apikey").await {
        return (StatusCode::SERVICE_UNAVAILABLE, "transient").into_response();
    }
    Json(serde_json::json!({
        "id": 1,
        "first_eight": "sk-full-",
        "note": null,
        "scopes": [],
        "expiration_time": null,
        "time_created": "2020-01-01T00:00:00Z",
        "latest_activity": null,
    }))
    .into_response()
}

async fn revoke_api_key(
    State(hits): State<Arc<Hits>>,
    Path(first_eight): Path<String>,
) -> impl IntoResponse {
    let key = format!("DELETE /auth/apikeys/{first_eight}");
    if hits.first(&key).await {
        return (StatusCode::SERVICE_UNAVAILABLE, "transient").into_response();
    }
    StatusCode::OK.into_response()
}

// --- metadata-write endpoint (base.rs) — PATCH/PUT/DELETE on one node ---

async fn metadata(State(hits): State<Arc<Hits>>, method: Method) -> impl IntoResponse {
    let key = format!("{method} /metadata/foo");
    if hits.first(&key).await {
        return (StatusCode::SERVICE_UNAVAILABLE, "transient").into_response();
    }
    StatusCode::OK.into_response()
}

#[tokio::test]
async fn transient_failure_retries_metadata_writes_and_auth_management() {
    let hits: Arc<Hits> = Arc::new(Hits::default());
    let app = Router::new()
        .route("/api/v1/auth/apikey", get(which_api_key))
        .route("/api/v1/auth/apikeys", post(create_api_key))
        .route("/api/v1/auth/apikeys/{first_eight}", delete(revoke_api_key))
        .route(
            "/api/v1/metadata/foo",
            patch(metadata).put(metadata).delete(metadata),
        )
        .with_state(hits.clone());
    let base = spawn(app).await;

    let (ctx, _) = Context::from_uri(&base).unwrap();

    // context.rs auth-management: each succeeds despite an initial 503.
    let created = ctx.create_api_key(None, None, None).await.unwrap();
    assert_eq!(created.secret, "sk-full-secret");
    let info = ctx.which_api_key().await.unwrap();
    assert_eq!(info.id, 1);
    ctx.revoke_api_key("abcdef12").await.unwrap();

    // base.rs metadata-writes: build a node whose `self` link is the mock
    // endpoint, then exercise PATCH (update_metadata), PUT (replace_metadata),
    // and DELETE (delete).
    let item = serde_json::from_value(serde_json::json!({
        "id": "foo",
        "attributes": { "ancestors": [], "structure_family": "container", "metadata": {} },
        "links": { "self": format!("{base}/api/v1/metadata/foo") }
    }))
    .unwrap();
    let node = ContainerClient::from_item(ctx.clone(), item, false).unwrap();
    node.base()
        .update_metadata(None, None, None, false)
        .await
        .unwrap();
    node.base()
        .replace_metadata(Some(serde_json::json!({})), None, None, false)
        .await
        .unwrap();
    node.base().delete(false, true).await.unwrap();

    // Every endpoint saw exactly two requests: the 503 plus the retry. A count
    // of 1 would mean the call was not wrapped in `retry` and failed fast.
    for key in [
        "POST /auth/apikeys",
        "GET /auth/apikey",
        "DELETE /auth/apikeys/abcdef12",
        "PATCH /metadata/foo",
        "PUT /metadata/foo",
        "DELETE /metadata/foo",
    ] {
        assert_eq!(hits.count(key).await, 2, "{key} must be retried once");
    }
}
