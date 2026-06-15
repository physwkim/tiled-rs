//! Integration tests for the /admin/login form — verifies that authentication
//! is actually performed before a session cookie is issued.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

async fn build_test_router() -> (axum::Router, tempfile::TempDir, tiled_auth::Issuer) {
    build_test_router_with(false).await
}

async fn build_test_router_with(
    trust_forwarded_proto: bool,
) -> (axum::Router, tempfile::TempDir, tiled_auth::Issuer) {
    let dir = tempfile::tempdir().unwrap();
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());
    let auth_db = tiled_auth::AuthDb::connect(&auth_uri).await.unwrap();
    auth_db.migrate().await.unwrap();

    let mut dummy = tiled_auth::DummyAuthenticator::new("dummy");
    dummy.add_user("alice", "s3cret").unwrap();

    let issuer = tiled_auth::Issuer::new(b"test-secret-that-is-32bytes-long!!").unwrap();

    let state = tiled_web::WebState {
        auth_db: Some(auth_db),
        issuer: Some(issuer.clone()),
        default_login_scopes: tiled_auth::ScopeSet::full(),
        login_provider: "dummy".into(),
        channel_count_fn: Arc::new(|| 0),
        trust_forwarded_proto,
        assets_dir: None,
        spec_views: Vec::new(),
        authenticator: Some(Arc::new(dummy)),
    };
    (tiled_web::build_router(state), dir, issuer)
}

/// Whether a Set-Cookie header carries the `Secure` attribute.
fn cookie_is_secure(resp: &axum::response::Response) -> bool {
    resp.headers()
        .get("set-cookie")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.contains("; Secure"))
}

async fn post_login_with_proto(
    app: &axum::Router,
    forwarded_proto: Option<&str>,
) -> axum::response::Response {
    let body = "provider=dummy&username=alice&password=s3cret";
    let mut req = Request::builder()
        .method(Method::POST)
        .uri("/admin/login")
        .header("content-type", "application/x-www-form-urlencoded");
    if let Some(proto) = forwarded_proto {
        req = req.header("x-forwarded-proto", proto);
    }
    app.clone()
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap()
}

/// Extract the `tiled_session` JWT from a Set-Cookie response header.
fn session_jwt(resp: &axum::response::Response) -> String {
    let raw = resp
        .headers()
        .get("set-cookie")
        .expect("set-cookie present")
        .to_str()
        .unwrap();
    raw.strip_prefix("tiled_session=")
        .and_then(|s| s.split(';').next())
        .expect("session cookie value")
        .to_string()
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
    let (app, _dir, _issuer) = build_test_router().await;
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
    let (app, _dir, _issuer) = build_test_router().await;
    let resp = post_login(&app, "dummy", "alice", "s3cret").await;
    assert!(
        resp.headers().get("set-cookie").is_some(),
        "session cookie must be set on successful authentication"
    );
}

#[tokio::test]
async fn secure_cookie_set_only_for_forwarded_https() {
    // Behind a trusted proxy (trust_forwarded_proto = true), the session
    // cookie gets `Secure` iff the proxy reports X-Forwarded-Proto: https.
    let (app, _dir, _issuer) = build_test_router_with(true).await;

    let https = post_login_with_proto(&app, Some("https")).await;
    assert!(
        cookie_is_secure(&https),
        "X-Forwarded-Proto: https must produce a Secure cookie"
    );

    let http = post_login_with_proto(&app, Some("http")).await;
    assert!(
        !cookie_is_secure(&http),
        "X-Forwarded-Proto: http must NOT produce a Secure cookie"
    );

    let none = post_login_with_proto(&app, None).await;
    assert!(
        !cookie_is_secure(&none),
        "absent X-Forwarded-Proto must NOT produce a Secure cookie"
    );
}

#[tokio::test]
async fn secure_cookie_never_set_without_trusted_proxy() {
    // With no trusted proxy (trust_forwarded_proto = false), a spoofed
    // X-Forwarded-Proto: https must NOT flip the cookie to Secure.
    let (app, _dir, _issuer) = build_test_router_with(false).await;
    let resp = post_login_with_proto(&app, Some("https")).await;
    assert!(
        !cookie_is_secure(&resp),
        "untrusted X-Forwarded-Proto must be ignored for the Secure flag"
    );
}

#[tokio::test]
async fn admin_page_sets_security_headers() {
    // Server-rendered admin HTML must carry nosniff + DENY framing.
    let (app, _dir, _issuer) = build_test_router().await;
    let req = Request::builder()
        .method(Method::GET)
        .uri("/admin/login")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("x-content-type-options").unwrap(),
        "nosniff"
    );
    assert_eq!(resp.headers().get("x-frame-options").unwrap(), "DENY");
}

#[tokio::test]
async fn api_key_create_rejects_non_numeric_expires_in() {
    // A typo'd expiry ("30d") must NOT silently mint a non-expiring key;
    // it must be rejected with a validation message.
    let (app, _dir, _issuer) = build_test_router().await;
    let login = post_login(&app, "dummy", "alice", "s3cret").await;
    let cookie = login
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/admin/api-keys/create")
        .header("cookie", cookie)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from("note=&scopes=&expires_in=30d"))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8_lossy(&body);
    assert!(
        html.contains("whole number of seconds"),
        "non-numeric expires_in must be rejected, not silently treated as never; body: {html}"
    );
}

#[tokio::test]
async fn streaming_page_requires_metrics_scope() {
    // alice (role "user") has no `metrics` scope even with default_login_scopes
    // = full() (the login cap drops it). /admin/streaming exposes server-global
    // channel counts, so it must refuse rather than leak them to her.
    let (app, _dir, _issuer) = build_test_router().await;
    let login = post_login(&app, "dummy", "alice", "s3cret").await;
    let cookie = login
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let req = Request::builder()
        .method(Method::GET)
        .uri("/admin/streaming")
        .header("cookie", cookie)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8_lossy(&body);
    assert!(
        html.contains("missing scope: metrics"),
        "streaming page must gate on the metrics scope for a non-metrics principal; body: {html}"
    );
}

#[tokio::test]
async fn login_caps_session_scopes_to_role() {
    // alice is a fresh principal → role "user" (no admin scope), while the
    // server's default_login_scopes is full(). The minted session must be
    // for_role("user") ∩ full() = for_role("user"), i.e. NOT include Admin.
    // Before the cap fix the session inherited the uncapped full() and so
    // carried Scope::Admin — a privilege escalation for a non-admin login.
    let (app, _dir, issuer) = build_test_router().await;
    let resp = post_login(&app, "dummy", "alice", "s3cret").await;
    let jwt = session_jwt(&resp);
    let claims = issuer.verify_access(&jwt).expect("session JWT verifies");
    assert!(
        !claims.scopes.contains(tiled_auth::Scope::Admin),
        "non-admin login must not be granted Admin scope, got {:?}",
        claims.scopes,
    );
    assert!(
        claims.scopes.contains(tiled_auth::Scope::ReadData),
        "role 'user' read scope must survive the cap, got {:?}",
        claims.scopes,
    );
}

#[tokio::test]
async fn unknown_user_issues_no_session_cookie() {
    let (app, _dir, _issuer) = build_test_router().await;
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
