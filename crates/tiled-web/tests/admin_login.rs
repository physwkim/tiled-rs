//! Integration tests for the /admin/login form — verifies that authentication
//! is actually performed before a session cookie is issued.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

async fn build_test_router() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());
    let auth_db = tiled_auth::AuthDb::connect(&auth_uri).await.unwrap();
    auth_db.migrate().await.unwrap();

    let mut dummy = tiled_auth::DummyAuthenticator::new("dummy");
    dummy.add_user("alice", "s3cret").unwrap();

    let issuer = tiled_auth::Issuer::new(b"test-secret-that-is-32bytes-long!!").unwrap();

    let state = tiled_web::WebState {
        auth_db: Some(auth_db),
        issuer: Some(issuer),
        default_login_scopes: tiled_auth::ScopeSet::full(),
        login_provider: "dummy".into(),
        channel_count_fn: Arc::new(|| 0),
        secure_cookies: false,
        assets_dir: None,
        spec_views: Vec::new(),
        authenticator: Some(Arc::new(dummy)),
    };
    (tiled_web::build_router(state), dir)
}

async fn post_login(
    app: &axum::Router,
    provider: &str,
    username: &str,
    password: &str,
) -> axum::response::Response {
    let body = format!("provider={provider}&username={username}&password={password}");
    let req = Request::builder()
        .method(Method::POST)
        .uri("/admin/login")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(body))
        .unwrap();
    app.clone().oneshot(req).await.unwrap()
}

#[tokio::test]
async fn wrong_password_issues_no_session_cookie() {
    let (app, _dir) = build_test_router().await;
    let resp = post_login(&app, "dummy", "alice", "wrongpassword").await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "login form must be re-rendered on failure"
    );
    assert!(
        resp.headers().get("set-cookie").is_none(),
        "no session cookie must be issued on failed authentication"
    );
}

#[tokio::test]
async fn correct_password_issues_session_cookie() {
    let (app, _dir) = build_test_router().await;
    let resp = post_login(&app, "dummy", "alice", "s3cret").await;
    assert!(
        resp.headers().get("set-cookie").is_some(),
        "session cookie must be set on successful authentication"
    );
}

#[tokio::test]
async fn unknown_user_issues_no_session_cookie() {
    let (app, _dir) = build_test_router().await;
    let resp = post_login(&app, "dummy", "nobody", "anything").await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "login form must be re-rendered for unknown user"
    );
    assert!(
        resp.headers().get("set-cookie").is_none(),
        "no session cookie must be issued for unknown username"
    );
}
