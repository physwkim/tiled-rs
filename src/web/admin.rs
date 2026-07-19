//! Server-rendered admin shell.

use std::sync::Arc;

use askama::Template;
use axum::Router;
use axum::extract::{Form, Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use chrono::{Duration, Utc};
use serde::Deserialize;
use sqlx::Row;

use crate::auth::{ApiKeyCreate, AuthDb, Principal, Scope, ScopeSet};

use crate::web::WebState;
use crate::web::cookie::{build_session_cookie, clear_session_cookie, read_session_cookie};

pub fn admin_router(state: WebState) -> Router {
    // Validate login_provider at construction time (once, during server startup).
    // Control chars or non-ASCII bytes would contaminate cookie values and header
    // strings if the field is ever interpolated into a Set-Cookie attribute.
    assert!(
        state
            .login_provider
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
        "login_provider must contain only ASCII alphanumeric, '_', or '-'; got {:?}",
        state.login_provider,
    );
    Router::new()
        .route("/admin/login", get(login_form).post(login_submit))
        .route("/admin/logout", post(logout_submit))
        .route("/admin/", get(redirect_to_keys))
        .route("/admin/api-keys", get(api_keys_page))
        .route("/admin/api-keys/create", post(api_keys_create))
        .route(
            "/admin/api-keys/{first_eight}/revoke",
            post(api_keys_revoke),
        )
        .route("/admin/sessions", get(sessions_page))
        .route("/admin/sessions/revoke-all", post(sessions_revoke_all))
        .route("/admin/streaming", get(streaming_page))
        .with_state(Arc::new(state))
}

async fn redirect_to_keys() -> Redirect {
    Redirect::temporary("/admin/api-keys")
}

/// Render an askama template into an axum Response. We don't use
/// askama_axum because it's pinned to axum 0.7 and we're on 0.8.
fn render<T: Template>(template: T) -> Response {
    match template.render() {
        Ok(html) => {
            let mut resp = Html(html).into_response();
            let h = resp.headers_mut();
            // Harden the server-rendered admin HTML: never sniff the type,
            // and refuse to be framed (clickjacking defence for the logout /
            // revoke / revoke-all POST forms — the admin panel is never meant
            // to be embedded).
            h.insert(
                header::X_CONTENT_TYPE_OPTIONS,
                HeaderValue::from_static("nosniff"),
            );
            h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
            resp
        }
        Err(e) => {
            // Don't echo internal template/render details to the browser.
            tracing::error!(error = %e, "admin template render failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------

#[derive(Template)]
#[template(path = "login.html")]
struct LoginTemplate {
    title: &'static str,
    message: Option<String>,
    login_error: Option<String>,
}

#[derive(Template)]
#[template(path = "api_keys.html")]
struct ApiKeysTemplate {
    title: &'static str,
    message: Option<String>,
    principal_uuid: String,
    scopes: String,
    keys: Vec<KeyRow>,
    new_secret: Option<String>,
}

struct KeyRow {
    first_eight: String,
    scopes: String,
    note: String,
    expires: String,
}

#[derive(Template)]
#[template(path = "sessions.html")]
struct SessionsTemplate {
    title: &'static str,
    message: Option<String>,
    principal_uuid: String,
    sessions: Vec<SessionRow>,
}

struct SessionRow {
    uuid: String,
    created: String,
    last_used: String,
    expires: String,
    revoked: bool,
}

#[derive(Template)]
#[template(path = "streaming.html")]
struct StreamingTemplate {
    title: &'static str,
    message: Option<String>,
    total_channels: usize,
}

// ---------------------------------------------------------------------------
// Login / logout
// ---------------------------------------------------------------------------

async fn login_form() -> Response {
    render(LoginTemplate {
        title: "sign in",
        message: None,
        login_error: None,
    })
}

#[derive(Debug, Deserialize)]
struct LoginForm {
    provider: String,
    username: String,
    password: String,
}

async fn login_submit(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Form(form): Form<LoginForm>,
) -> Response {
    let Some(db) = state.auth_db.clone() else {
        return error_login("auth DB not configured on this server");
    };
    let Some(issuer) = state.issuer.clone() else {
        return error_login("JWT issuer not configured");
    };
    if form.provider != state.login_provider {
        return error_login(&format!(
            "unknown auth provider '{}'; configured: {}",
            form.provider, state.login_provider
        ));
    }
    let Some(auth) = state.authenticator.as_ref() else {
        return error_login("no authenticator configured");
    };
    let subject = match auth.authenticate(&form.username, &form.password).await {
        Ok(s) => s,
        Err(_) => return error_login("invalid username or password"),
    };
    let (principal, identity) = match db.ensure_principal(&subject.provider, &subject.sub).await {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, "admin login: ensure_principal failed");
            return error_login("login failed");
        }
    };
    db.touch_identity_login(identity.id).await.ok();
    // Cap the minted scopes by the principal's role, exactly like the API
    // login path (`mint_session_scopes`, app.rs) and the other four session
    // mint sites: `for_role(role) ∩ default_login_scopes`. Using the cap
    // directly would let a broadened `default_login_scopes` hand a low-role
    // principal more than its role allows (latent privilege escalation).
    let scopes = ScopeSet::for_role(&principal.role).intersect(&state.default_login_scopes);
    // Absolute session cap (`session_max_age`, default 365 d) — NOT the 7-day
    // refresh-token TTL. Matches the API session-mint sites (auth_router.rs).
    let session_ttl = issuer.session_ttl;
    let session = match db
        .create_session(
            principal.id,
            scopes.clone(),
            Utc::now() + session_ttl,
            // Admin password login: no upstream IdP tokens to carry.
            serde_json::json!({}),
        )
        .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "admin login: create_session failed");
            return error_login("login failed");
        }
    };
    let access_token = match issuer.issue_access(
        &principal.uuid,
        &session.uuid,
        scopes,
        session.state.clone(),
    ) {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(error = %e, "admin login: issue_access failed");
            return error_login("login failed");
        }
    };
    let cookie = build_session_cookie(
        &access_token,
        issuer.access_ttl.num_seconds(),
        cookie_is_secure(&state, &headers),
    );
    let mut response = Redirect::temporary("/admin/api-keys").into_response();
    response.headers_mut().insert(header::SET_COOKIE, cookie);
    response
}

async fn logout_submit(State(state): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    if let (Some(db), Some(issuer), Some(jwt)) = (
        state.auth_db.clone(),
        state.issuer.clone(),
        read_session_cookie(&headers),
    ) && let Ok(claims) = issuer.verify_access(&jwt)
    {
        // Revoke the specific session, not every session for the
        // principal — the API's logout endpoint already supports
        // logout-everywhere if the user wants it.
        db.revoke_session(&claims.sid).await.ok();
    }
    let cookie = clear_session_cookie(cookie_is_secure(&state, &headers));
    let mut response = Redirect::temporary("/admin/login").into_response();
    response.headers_mut().insert(header::SET_COOKIE, cookie);
    response
}

/// Decide the session cookie's `Secure` flag for this request. The server
/// never terminates TLS, so the only HTTPS signal is a fronting proxy's
/// `X-Forwarded-Proto` header — honored only when the host trusts forwarded
/// headers (`trust_forwarded_proto`). A plain-HTTP request (or one with no
/// trusted proxy) yields `false`, so the admin login still works over HTTP.
fn cookie_is_secure(state: &WebState, headers: &HeaderMap) -> bool {
    state.trust_forwarded_proto
        && headers
            .get("x-forwarded-proto")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|s| s.eq_ignore_ascii_case("https"))
}

fn error_login(msg: &str) -> Response {
    render(LoginTemplate {
        title: "sign in",
        message: None,
        login_error: Some(msg.to_string()),
    })
}

// ---------------------------------------------------------------------------
// API keys
// ---------------------------------------------------------------------------

async fn api_keys_page(State(state): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    let session = match resolve_session(&state, &headers).await {
        Ok(s) => s,
        Err(redir) => return redir,
    };
    render_api_keys(&state, &session, None, None).await
}

#[derive(Debug, Deserialize)]
struct ApiKeyForm {
    note: String,
    scopes: String,
    expires_in: String,
}

async fn api_keys_create(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Form(form): Form<ApiKeyForm>,
) -> Response {
    let session = match resolve_session(&state, &headers).await {
        Ok(s) => s,
        Err(redir) => return redir,
    };
    let Some(db) = state.auth_db.clone() else {
        return render_api_keys(
            &state,
            &session,
            Some("auth DB not configured".into()),
            None,
        )
        .await;
    };
    if !session.scopes.contains(Scope::CreateApiKeys) {
        return render_api_keys(
            &state,
            &session,
            Some("missing scope: create:apikeys".into()),
            None,
        )
        .await;
    }
    // Resolve requested scopes through the shared owner
    // (`resolve_apikey_scopes`) so this self-service form matches the JSON
    // `POST /auth/apikeys` route and upstream `generate_apikey`: an empty box
    // defaults to `["inherit"]` (NOT a frozen snapshot of the caller's current
    // session scopes), and each named scope is capped by the principal's ROLE
    // ceiling (NOT the caller's possibly-narrower session scopes).
    let requested: Option<Vec<String>> = if form.scopes.trim().is_empty() {
        None
    } else {
        Some(
            form.scopes
                .split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect(),
        )
    };
    let scopes =
        match crate::server::auth_router::resolve_apikey_scopes(requested, &session.principal.role)
        {
            Ok(set) => set,
            Err(e) => {
                return render_api_keys(&state, &session, Some(e.to_string()), None).await;
            }
        };
    // Empty means "never expires". A non-empty value MUST parse to a
    // positive number of seconds — previously any parse error (a typo like
    // "30d", or a stray word) silently fell through to None, minting a
    // non-expiring key the operator didn't intend, and a negative value
    // produced a key already expired in the past. Validate explicitly.
    let expires_in = form.expires_in.trim();
    let exp = if expires_in.is_empty() {
        None
    } else {
        match expires_in.parse::<i64>() {
            Ok(s) if s > 0 => Some(Utc::now() + Duration::seconds(s)),
            Ok(_) => {
                return render_api_keys(
                    &state,
                    &session,
                    Some(
                        "expires_in must be a positive number of seconds (or empty for never)"
                            .into(),
                    ),
                    None,
                )
                .await;
            }
            Err(_) => {
                return render_api_keys(
                    &state,
                    &session,
                    Some(
                        "expires_in must be a whole number of seconds (or empty for never)".into(),
                    ),
                    None,
                )
                .await;
            }
        }
    };
    let note = if form.note.trim().is_empty() {
        None
    } else {
        Some(form.note.trim().to_string())
    };
    let material = match db
        .create_api_key(ApiKeyCreate {
            principal_id: session.principal.id,
            note,
            scopes,
            expiration_time: exp,
            // The admin SPA key form does not accept a tag restriction.
            access_tags: None,
        })
        .await
    {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "admin: create_api_key failed");
            return render_api_keys(
                &state,
                &session,
                Some("failed to create API key".into()),
                None,
            )
            .await;
        }
    };
    render_api_keys(&state, &session, None, Some(material.secret)).await
}

async fn api_keys_revoke(
    State(state): State<Arc<WebState>>,
    headers: HeaderMap,
    Path(first_eight): Path<String>,
) -> Response {
    let session = match resolve_session(&state, &headers).await {
        Ok(s) => s,
        Err(redir) => return redir,
    };
    let Some(db) = state.auth_db.clone() else {
        return render_api_keys(
            &state,
            &session,
            Some("auth DB not configured".into()),
            None,
        )
        .await;
    };
    if !session.scopes.contains(Scope::RevokeApiKeys) {
        return render_api_keys(
            &state,
            &session,
            Some("missing scope: revoke:apikeys".into()),
            None,
        )
        .await;
    }
    // Ownership check (mirrors the API endpoint).
    let owned = db
        .list_api_keys(Some(session.principal.id))
        .await
        .unwrap_or_default();
    let is_admin = session.scopes.contains(Scope::AdminApiKeys);
    let allowed = owned.iter().any(|k| k.first_eight == first_eight) || is_admin;
    if !allowed {
        return render_api_keys(
            &state,
            &session,
            Some("api key does not belong to this principal".into()),
            None,
        )
        .await;
    }
    let caller_id = if is_admin {
        None
    } else {
        Some(session.principal.id)
    };
    if let Err(e) = db.revoke_api_key(&first_eight, caller_id).await {
        tracing::error!(error = %e, "admin: revoke_api_key failed");
        return render_api_keys(
            &state,
            &session,
            Some("failed to revoke API key".into()),
            None,
        )
        .await;
    }
    render_api_keys(
        &state,
        &session,
        Some(format!("revoked key {first_eight}")),
        None,
    )
    .await
}

async fn render_api_keys(
    state: &WebState,
    session: &SessionContext,
    message: Option<String>,
    new_secret: Option<String>,
) -> Response {
    let Some(db) = state.auth_db.clone() else {
        return render(ApiKeysTemplate {
            title: "API keys",
            message: Some("auth DB not configured".into()),
            principal_uuid: session.principal.uuid.clone(),
            scopes: session
                .scopes
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            keys: vec![],
            new_secret: None,
        });
    };
    let keys = db
        .list_api_keys(Some(session.principal.id))
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|k| KeyRow {
            first_eight: k.first_eight,
            scopes: k
                .scopes
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            note: k.note.unwrap_or_default(),
            expires: k
                .expiration_time
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| "never".into()),
        })
        .collect();
    render(ApiKeysTemplate {
        title: "API keys",
        message,
        principal_uuid: session.principal.uuid.clone(),
        scopes: session
            .scopes
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        keys,
        new_secret,
    })
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

async fn sessions_page(State(state): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    let session = match resolve_session(&state, &headers).await {
        Ok(s) => s,
        Err(redir) => return redir,
    };
    let Some(db) = state.auth_db.clone() else {
        return render(SessionsTemplate {
            title: "sessions",
            message: Some("auth DB not configured".into()),
            principal_uuid: session.principal.uuid.clone(),
            sessions: vec![],
        });
    };
    let rows = list_sessions_for_principal(&db, session.principal.id).await;
    render(SessionsTemplate {
        title: "sessions",
        message: None,
        principal_uuid: session.principal.uuid.clone(),
        sessions: rows,
    })
}

async fn sessions_revoke_all(State(state): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    let session = match resolve_session(&state, &headers).await {
        Ok(s) => s,
        Err(redir) => return redir,
    };
    if let Some(db) = state.auth_db.clone() {
        let _ = db.revoke_all_sessions(session.principal.id).await;
    }
    let mut response = Redirect::temporary("/admin/login").into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        clear_session_cookie(cookie_is_secure(&state, &headers)),
    );
    response
}

async fn list_sessions_for_principal(db: &AuthDb, principal_id: i64) -> Vec<SessionRow> {
    use crate::auth::db::AuthPool;
    let mut out = Vec::new();
    match db.pool() {
        AuthPool::Sqlite(pool) => {
            if let Ok(rows) = sqlx::query(
                "SELECT uuid, time_created, time_last_used, expiration_time, revoked
                   FROM sessions WHERE principal_id = ? ORDER BY time_created DESC",
            )
            .bind(principal_id)
            .fetch_all(pool)
            .await
            {
                for row in rows {
                    out.push(SessionRow {
                        uuid: row.get("uuid"),
                        created: row.get("time_created"),
                        last_used: row
                            .try_get::<String, _>("time_last_used")
                            .unwrap_or_default(),
                        expires: row.get("expiration_time"),
                        revoked: row.get::<i64, _>("revoked") != 0,
                    });
                }
            }
        }
        AuthPool::Postgres(pool) => {
            if let Ok(rows) = sqlx::query(
                "SELECT uuid, time_created, time_last_used, expiration_time, revoked
                   FROM sessions WHERE principal_id = $1 ORDER BY time_created DESC",
            )
            .bind(principal_id)
            .fetch_all(pool)
            .await
            {
                for row in rows {
                    let created: chrono::DateTime<chrono::Utc> = row.get("time_created");
                    let expires: chrono::DateTime<chrono::Utc> = row.get("expiration_time");
                    let last_used: Option<chrono::DateTime<chrono::Utc>> =
                        row.try_get("time_last_used").ok();
                    out.push(SessionRow {
                        uuid: row.get("uuid"),
                        created: created.to_rfc3339(),
                        last_used: last_used.map(|t| t.to_rfc3339()).unwrap_or_default(),
                        expires: expires.to_rfc3339(),
                        revoked: row.get("revoked"),
                    });
                }
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Streaming
// ---------------------------------------------------------------------------

async fn streaming_page(State(state): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    let session = match resolve_session(&state, &headers).await {
        Ok(s) => s,
        Err(redir) => return redir,
    };
    // Unlike the api-keys/sessions pages (which only ever show the caller's
    // OWN keys/sessions), this page exposes server-global state — the live
    // count of every streaming channel. Gate it on the `metrics` scope so a
    // low-privilege principal can't read infra internals; the other admin
    // pages need no such gate because their data is already principal-scoped.
    if !session.scopes.contains(Scope::Metrics) {
        return render(StreamingTemplate {
            title: "streaming",
            message: Some("missing scope: metrics".into()),
            total_channels: 0,
        });
    }
    let total = (state.channel_count_fn)();
    render(StreamingTemplate {
        title: "streaming",
        message: None,
        total_channels: total,
    })
}

// ---------------------------------------------------------------------------
// Session resolution
// ---------------------------------------------------------------------------

struct SessionContext {
    principal: Principal,
    scopes: ScopeSet,
}

async fn resolve_session(
    state: &WebState,
    headers: &HeaderMap,
) -> Result<SessionContext, Response> {
    let Some(db) = state.auth_db.clone() else {
        return Err(Redirect::temporary("/admin/login").into_response());
    };
    let Some(issuer) = state.issuer.clone() else {
        return Err(Redirect::temporary("/admin/login").into_response());
    };
    let Some(jwt) = read_session_cookie(headers) else {
        return Err(Redirect::temporary("/admin/login").into_response());
    };
    let claims = issuer
        .verify_access(&jwt)
        .map_err(|_| Redirect::temporary("/admin/login").into_response())?;
    let session = db
        .lookup_session(&claims.sid)
        .await
        .map_err(|_| Redirect::temporary("/admin/login").into_response())?;
    if session.revoked || session.expiration_time <= Utc::now() {
        return Err((StatusCode::UNAUTHORIZED, "session expired").into_response());
    }
    let principal = db
        .get_principal(session.principal_id)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "principal lookup").into_response())?
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "principal missing").into_response())?;
    Ok(SessionContext {
        principal,
        scopes: claims.scopes.intersect(&session.scopes),
    })
}
