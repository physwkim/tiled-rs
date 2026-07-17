//! Wave-27 batch-3, Finding 2: `configure_auth(remember_me = false)` must drop
//! any tokens a previous `remember_me = true` session cached on disk for this
//! server, not merely skip persisting the new ones. Otherwise a later
//! `use_cached_tokens()` could resurrect a session the caller explicitly chose
//! not to remember. Mirrors upstream `Context.configure_auth`
//! (`context.py:1006-1013`), which clears `access_token` and `refresh_token`.
//!
//! This is the only test in this binary and the only place that sets
//! `TILED_CACHE_DIR`, so the single-threaded env mutation races with nothing.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::header::SET_COOKIE;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::routing::post;
use tokio::net::TcpListener;

use tiled_rs::client::{Context, Tokens, token_directory_for_server};

fn about_payload() -> serde_json::Value {
    serde_json::json!({
        "api_version": 0,
        "library_version": "test",
        "formats": {},
        "aliases": {},
        "queries": [],
        "authentication": {
            "required": false,
            "providers": [],
            "links": { "refresh_session": "/auth/session/refresh" }
        },
        "links": {},
        "meta": {},
    })
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

#[tokio::test]
async fn remember_me_false_clears_previously_cached_tokens() {
    let cache_dir = tempfile::tempdir().unwrap();
    // SAFETY: this is the only test in this binary and the only setter/reader of
    // TILED_CACHE_DIR here, so there is no concurrent getenv to race with.
    unsafe {
        std::env::set_var("TILED_CACHE_DIR", cache_dir.path());
    }

    // Server hands back a tiled_csrf cookie so configure_auth's csrf check
    // passes and advertises a refresh_session link.
    async fn handle_about(State(counter): State<Arc<()>>) -> impl IntoResponse {
        let _ = counter;
        (
            [(SET_COOKIE, "tiled_csrf=csrf-token; Path=/")],
            Json(about_payload()),
        )
    }
    let app = Router::new()
        .route("/api/v1/", get(handle_about))
        .route(
            "/api/v1/auth/session/refresh",
            post(|| async { Json(serde_json::json!({})) }),
        )
        .with_state(Arc::new(()));
    let base = spawn(app).await;

    let (ctx, _) = Context::from_uri(&base).unwrap();
    // Populate server_info and capture the csrf cookie.
    ctx.server_info().await.unwrap();

    let token_dir = token_directory_for_server(ctx.api_uri());
    let access = token_dir.join("access_token");
    let refresh = token_dir.join("refresh_token");

    // 1) remember_me = true: the tokens land on disk for this server.
    ctx.configure_auth(
        Tokens {
            access_token: "cached-access".into(),
            refresh_token: "cached-refresh".into(),
            id_token: None,
        },
        true,
    )
    .await
    .unwrap();
    assert!(
        access.exists(),
        "remember_me=true must persist access_token"
    );
    assert!(
        refresh.exists(),
        "remember_me=true must persist refresh_token"
    );

    // 2) remember_me = false: the previously cached tokens must be dropped.
    ctx.configure_auth(
        Tokens {
            access_token: "ephemeral-access".into(),
            refresh_token: "ephemeral-refresh".into(),
            id_token: None,
        },
        false,
    )
    .await
    .unwrap();
    assert!(
        !access.exists(),
        "remember_me=false must clear the previously cached access_token"
    );
    assert!(
        !refresh.exists(),
        "remember_me=false must clear the previously cached refresh_token"
    );

    // The new tokens are still active in memory — the session is not logged out,
    // only un-remembered.
    assert!(
        ctx.authenticated().await,
        "the ephemeral tokens must remain configured in memory"
    );
}
